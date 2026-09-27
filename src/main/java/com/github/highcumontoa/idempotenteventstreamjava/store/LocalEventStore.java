package com.github.highcumontoa.idempotenteventstreamjava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventstreamjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventstreamjava.model.FailureClass;
import com.github.highcumontoa.idempotenteventstreamjava.service.EventStreamProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存索引 + 本地文件的存储实现。
 *
 * <p>持久化布局（{@code event.stream.state-dir} 配置）：
 * <ul>
 *   <li>{@code journals/<aggregateId>.jsonl}：该聚合事件的追加日志，每行一条事件；</li>
 *   <li>{@code checkpoints.properties}：每个聚合最后已生效的序号，临时文件 + 原子替换写入。</li>
 * </ul>
 *
 * <p>启动时从日志重建内存索引：序号不大于检查点的事件直接视为已生效（SUCCEEDED），
 * 其余恢复为 WAITING 等待按序重放——保证重启后进度不倒退，且不重复生效副作用。
 */
@Component
public class LocalEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(LocalEventStore.class);

    private record StoredEvent(String key, String aggregate, long seq, String payload,
                               String hash, long createdAt, String result) {
    }

    private final EventStreamProperties properties;
    private final ObjectMapper mapper = new ObjectMapper();

    private final Map<String, EventRecord> byKey = new ConcurrentHashMap<>();
    private final Map<String, Map<Long, EventRecord>> byAggregate = new ConcurrentHashMap<>();
    private final Map<String, Long> checkpoints = new ConcurrentHashMap<>();

    private Path journalsDir;
    private Path checkpointFile;

    public LocalEventStore(EventStreamProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void init() {
        Path stateDir = Path.of(properties.getStateDir());
        this.journalsDir = stateDir.resolve("journals");
        this.checkpointFile = stateDir.resolve("checkpoints.properties");
        try {
            Files.createDirectories(journalsDir);
            loadCheckpoints();
            loadJournals();
            log.info("LocalEventStore 初始化完成 stateDir={} 事件数={} 检查点={}",
                    stateDir.toAbsolutePath(), byKey.size(), checkpoints);
        } catch (IOException e) {
            throw new UncheckedIOException("初始化本地状态失败: " + stateDir, e);
        }
    }

    @Override
    public Optional<EventRecord> findByIdempotencyKey(String key) {
        return Optional.ofNullable(byKey.get(key));
    }

    @Override
    public Optional<EventRecord> findBySeq(String aggregateId, long seqNum) {
        Map<Long, EventRecord> map = byAggregate.get(aggregateId);
        return map == null ? Optional.empty() : Optional.ofNullable(map.get(seqNum));
    }

    @Override
    public void insert(EventRecord record) {
        appendJournal(record);
        indexInsert(record);
    }

    @Override
    public void persistResult(EventRecord record) {
        appendJournal(record);
    }

    private void indexInsert(EventRecord record) {
        byKey.put(record.idempotencyKey(), record);
        byAggregate.computeIfAbsent(record.aggregateId(), k -> new ConcurrentHashMap<>())
                .put(record.seqNum(), record);
    }

    @Override
    public long checkpoint(String aggregateId) {
        return checkpoints.getOrDefault(aggregateId, 0L);
    }

    @Override
    public void advanceCheckpoint(String aggregateId, long seqNum) {
        synchronized (checkpoints) {
            long current = checkpoints.getOrDefault(aggregateId, 0L);
            if (seqNum <= current) {
                // 防御：检查点只进不退
                throw new IllegalStateException(
                        "检查点不允许回退 aggregate=" + aggregateId
                                + " current=" + current + " requested=" + seqNum);
            }
            checkpoints.put(aggregateId, seqNum);
            persistCheckpoints();
        }
    }

    @Override
    public List<EventRecord> waitingOrdered(String aggregateId) {
        Map<Long, EventRecord> map = byAggregate.get(aggregateId);
        if (map == null) {
            return List.of();
        }
        return map.values().stream()
                .filter(r -> r.status() == EventStatus.WAITING)
                .sorted(Comparator.comparingLong(EventRecord::seqNum))
                .toList();
    }

    @Override
    public List<EventRecord> all() {
        return List.copyOf(byKey.values());
    }

    // ------------------------------------------------------------------
    // 落盘 / 回放
    // ------------------------------------------------------------------

    private Path journalPath(String aggregateId) {
        // 聚合 id 直接作为文件名；做最小净化，避免路径分隔符
        String safe = aggregateId.replaceAll("[^a-zA-Z0-9._-]", "_");
        return journalsDir.resolve(safe + ".jsonl");
    }

    private void appendJournal(EventRecord record) {
        StoredEvent stored = new StoredEvent(
                record.idempotencyKey(), record.aggregateId(), record.seqNum(),
                record.payload(), record.contentHash(), record.createdAtMillis(),
                record.resultValue());
        String line;
        try {
            line = mapper.writeValueAsString(stored);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (BufferedWriter writer = Files.newBufferedWriter(
                journalPath(record.aggregateId()), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            synchronized (journalLock(record.aggregateId())) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("写入事件日志失败 aggregate=" + record.aggregateId(), e);
        }
    }

    private final Map<String, Object> journalLocks = new ConcurrentHashMap<>();

    private Object journalLock(String aggregateId) {
        return journalLocks.computeIfAbsent(aggregateId, k -> new Object());
    }

    private void loadCheckpoints() throws IOException {
        if (!Files.exists(checkpointFile)) {
            return;
        }
        Properties props = new Properties();
        try (var in = Files.newBufferedReader(checkpointFile, StandardCharsets.UTF_8)) {
            props.load(in);
        }
        for (String name : props.stringPropertyNames()) {
            checkpoints.put(name, Long.parseLong(props.getProperty(name)));
        }
    }

    private void persistCheckpoints() {
        Properties props = new Properties();
        checkpoints.forEach((k, v) -> props.setProperty(k, Long.toString(v)));
        Path tmp = checkpointFile.resolveSibling(checkpointFile.getFileName() + ".tmp");
        try (var out = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
            props.store(out, "event-stream checkpoints");
        } catch (IOException e) {
            throw new UncheckedIOException("持久化检查点失败", e);
        }
        try {
            Files.move(tmp, checkpointFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // 某些文件系统不支持 ATOMIC_MOVE，退回普通替换
            try {
                Files.move(tmp, checkpointFile, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                throw new UncheckedIOException("原子替换检查点文件失败", ex);
            }
        }
    }

    private void loadJournals() throws IOException {
        if (!Files.exists(journalsDir)) {
            return;
        }
        try (var paths = Files.list(journalsDir)) {
            for (Path journal : paths.filter(p -> p.toString().endsWith(".jsonl")).toList()) {
                long cp = checkpoints.getOrDefault(aggregateOf(journal), 0L);
                for (String line : Files.readAllLines(journal, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    StoredEvent stored = mapper.readValue(line, StoredEvent.class);
                    EventRecord existingRecord = byKey.get(stored.key());
                    if (existingRecord != null) {
                        // 成功结果行（同一键的第二行）：合并结果值
                        if (stored.result() != null) {
                            existingRecord.restoreSucceeded(stored.result());
                        }
                        continue;
                    }
                    EventRecord record = new EventRecord(
                            stored.key(), stored.aggregate(), stored.seq(),
                            stored.payload(), stored.hash());
                    if (stored.seq() <= cp) {
                        // 已生效：还原首次处理结果，不重放副作用；
                        // 异常缺结果行时回退占位值
                        record.restoreSucceeded(
                                stored.result() != null ? stored.result() : "restored:" + stored.key());
                    }
                    indexInsert(record);
                }
            }
        }
    }

    private String aggregateOf(Path journal) {
        String name = journal.getFileName().toString();
        return name.substring(0, name.length() - ".jsonl".length());
    }
}
