package com.github.lisama569.lakehousesnapshotservicejava.engine;

import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.model.DataType;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.model.RowRecord;
import com.github.lisama569.lakehousesnapshotservicejava.storage.TableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 行数据的校验、类型转换与按列 id 编解码。
 *
 * <p>数据文件每行 JSON 形如 {@code {"v":3,"c":{"1":"a","2":17}}}，其中 v 为写入时的
 * 模式版本，c 的键是列 id（不是列名）。列重命名 id 不变，旧文件因此不会错列；
 * 旧文件缺少新增列 id 时解码补 null 并在结果中标注 defaultFilledColumns。</p>
 */
public final class RowCodec {

    private static final Logger log = LoggerFactory.getLogger(RowCodec.class);

    private RowCodec() {
    }

    /**
     * 校验入参行并完成安全的标量转换：
     * <ul>
     *   <li>未知列名 -> UNKNOWN_COLUMN；</li>
     *   <li>主键列缺失或为 null -> MISSING_PRIMARY_KEY；</li>
     *   <li>值与声明类型不符 -> TYPE_MISMATCH（数字字符串允许转 LONG/DOUBLE，规则可解释）；</li>
     *   <li>普通列缺失 -> 填 null。</li>
     * </ul>
     * 返回保持列顺序的 列名->值 映射。
     */
    public static Map<String, Object> validateAndNormalize(TableState state, RowRecord row) {
        Map<String, Object> input = row.values() == null ? Map.of() : row.values();
        Map<String, ColumnDef> byName = byName(state);

        for (String key : input.keySet()) {
            if (!byName.containsKey(key)) {
                throw new LakehouseException(RejectReason.UNKNOWN_COLUMN,
                        "行包含未知列 '" + key + "'，当前模式列=" + byName.keySet());
            }
        }

        Map<String, Object> normalized = new LinkedHashMap<>();
        for (ColumnDef col : state.columns) {
            Object raw = input.get(col.name());
            Object converted;
            if (raw == null) {
                converted = null;
            } else {
                converted = convert(col, raw);
            }
            normalized.put(col.name(), converted);
        }

        for (String pk : state.primaryKey) {
            Object v = normalized.get(pk);
            if (v == null) {
                throw new LakehouseException(RejectReason.MISSING_PRIMARY_KEY,
                        "主键列 '" + pk + "' 缺失或为 null");
            }
        }
        return normalized;
    }

    private static Object convert(ColumnDef col, Object raw) {
        try {
            return switch (col.type()) {
                case STRING -> {
                    if (raw instanceof String s) {
                        yield s;
                    }
                    throw new LakehouseException(RejectReason.TYPE_MISMATCH, mismatchMsg(col, raw));
                }
                case BOOLEAN -> {
                    if (raw instanceof Boolean b) {
                        yield b;
                    }
                    throw new LakehouseException(RejectReason.TYPE_MISMATCH, mismatchMsg(col, raw));
                }
                case LONG -> {
                    if (raw instanceof Number n) {
                        long lv = n.longValue();
                        if (n.doubleValue() != (double) lv) {
                            throw new LakehouseException(RejectReason.TYPE_MISMATCH,
                                    mismatchMsg(col, raw));
                        }
                        yield lv;
                    }
                    if (raw instanceof String s) {
                        yield Long.parseLong(s.trim());
                    }
                    throw new LakehouseException(RejectReason.TYPE_MISMATCH, mismatchMsg(col, raw));
                }
                case DOUBLE -> {
                    if (raw instanceof Number n) {
                        yield n.doubleValue();
                    }
                    if (raw instanceof String s) {
                        yield Double.parseDouble(s.trim());
                    }
                    throw new LakehouseException(RejectReason.TYPE_MISMATCH, mismatchMsg(col, raw));
                }
            };
        } catch (NumberFormatException e) {
            throw new LakehouseException(RejectReason.TYPE_MISMATCH, mismatchMsg(col, raw));
        }
    }

    private static String mismatchMsg(ColumnDef col, Object raw) {
        return "列 '" + col.name() + "' 期望类型 " + col.type()
                + "，实际值=" + raw + " (" + raw.getClass().getSimpleName() + ")";
    }

    /** 逻辑行 -> 存储行：{"v":schemaVersion,"c":{"<colId>":value,...}}，所有当前列（含 null）都会写出。 */
    public static Map<String, Object> encode(TableState state, Map<String, Object> normalized) {
        Map<String, Object> c = new LinkedHashMap<>();
        for (ColumnDef col : state.columns) {
            c.put(Integer.toString(col.id()), normalized.get(col.name()));
        }
        return Map.of("v", state.schemaVersion, "c", c);
    }

    /**
     * 存储行 -> 当前模式下的逻辑行。
     * 旧数据缺少的列（即新增列）补 null 并记录列名；已删除列的残留字段被忽略。
     */
    public static DecodedRow decode(TableState state, Map<String, Object> encoded) {
        Object cObj = encoded.get("c");
        @SuppressWarnings("unchecked")
        Map<String, Object> c = cObj instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();

        Map<String, Object> values = new LinkedHashMap<>();
        List<String> defaultFilled = new ArrayList<>();
        for (ColumnDef col : state.columns) {
            String key = Integer.toString(col.id());
            if (c.containsKey(key)) {
                values.put(col.name(), coerce(col.type(), c.get(key)));
            } else {
                // 写入时的编码包含当时模式的全部列，因此缺失只能来自“后来新增的列”。
Object dflt = state.columnDefault(col.id());
                values.put(col.name(), coerce(col.type(), dflt));
                defaultFilled.add(col.name());
            }
        }
        if (!defaultFilled.isEmpty()) {
            log.debug("[lakehouse] decode-default-fill columns={}", defaultFilled);
        }
        return new DecodedRow(values, defaultFilled);
    }

    static Map<String, ColumnDef> byName(TableState state) {
        Map<String, ColumnDef> m = new LinkedHashMap<>();
        for (ColumnDef c : state.columns) {
            m.put(c.name(), c);
        }
        return m;
    }

    /** 回读时按声明类型规整（JSON 会把小整数解析成 Integer，统一转回目标标量类型）。 */
    private static Object coerce(DataType type, Object value) {
        if (value == null) {
            return null;
        }
        return switch (type) {
            case LONG -> value instanceof Number n ? n.longValue() : value;
            case DOUBLE -> value instanceof Number n ? n.doubleValue() : value;
            default -> value;
        };
    }

    public record DecodedRow(Map<String, Object> values, List<String> defaultFilledColumns) {
    }
}
