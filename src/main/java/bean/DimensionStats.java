package bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DimensionStats {

    // 缺陷尺寸总数 检验总尺寸数 缺陷尺寸总种数 复检尺寸总数 复检不合格尺寸总数

    public DimensionStats(String wbt, String wet,String mid, String fid, String lid, String mType, String eId, String isNew, Long defect_sum, Long check_sum, Long defect_type_count, Long double_sum, Long double_fail_sum) {
        this.wbt = wbt;
        this.wet = wet;
        this.mid = mid;
        this.fid = fid;
        this.lid = lid;
        this.mType = mType;
        this.eId = eId;
        this.isNew = isNew;
        this.defect_sum = defect_sum;
        this.check_sum = check_sum;
        this.defect_type_count = defect_type_count;
        this.double_sum = double_sum;
        this.double_fail_sum = double_fail_sum;
    }
    // 窗口开始时间 窗口结束时间 工厂维度 产线维度 工艺类型维度 生产设备维度 是否是新尺寸

    private String wbt; //窗口开始时间
    private String wet; //窗口结束时间
    private String mid; //制造id
    private String fid; //工厂id
    private String lid; //产线id
    private String mType; //制造类型
    private String eId; //设备id
    private String isNew; //是否是新尺寸
    private Long defect_sum; //缺陷尺寸总数
    private Long check_sum; //检验尺寸总数
    private Long defect_type_count; //缺陷尺寸总种数
    private Long double_sum; //复检尺寸总数
    private Long double_fail_sum; //复检不合格石村总数
    private Long ts; //消息产生时间

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }

    public String getWbt() {
        return wbt;
    }

    public void setWbt(String wbt) {
        this.wbt = wbt;
    }

    public String getWet() {
        return wet;
    }

    public void setWet(String wet) {
        this.wet = wet;
    }

    public String getFid() {
        return fid;
    }

    public void setFid(String fid) {
        this.fid = fid;
    }

    public String getLid() {
        return lid;
    }

    public void setLid(String lid) {
        this.lid = lid;
    }

    public String getmType() {
        return mType;
    }

    public void setmType(String mType) {
        this.mType = mType;
    }

    public String geteId() {
        return eId;
    }

    public void seteId(String eId) {
        this.eId = eId;
    }

    public String getIsNew() {
        return isNew;
    }

    public void setIsNew(String isNew) {
        this.isNew = isNew;
    }

    public Long getDefect_sum() {
        return defect_sum;
    }

    public void setDefect_sum(Long defect_sum) {
        this.defect_sum = defect_sum;
    }

    public Long getCheck_sum() {
        return check_sum;
    }

    public void setCheck_sum(Long check_sum) {
        this.check_sum = check_sum;
    }

    public Long getDefect_type_count() {
        return defect_type_count;
    }

    public void setDefect_type_count(Long defect_type_count) {
        this.defect_type_count = defect_type_count;
    }

    public Long getDouble_sum() {
        return double_sum;
    }

    public void setDouble_sum(Long double_sum) {
        this.double_sum = double_sum;
    }

    public Long getDouble_fail_sum() {
        return double_fail_sum;
    }

    public void setDouble_fail_sum(Long double_fail_sum) {
        this.double_fail_sum = double_fail_sum;
    }
}
