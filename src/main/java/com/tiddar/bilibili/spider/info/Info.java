package com.tiddar.bilibili.spider.info;

import com.google.gson.Gson;
import lombok.Data;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

@Data
public class Info {
    private static Gson gson = new Gson();

    private Integer aid;//aid
    private Integer view;//播放次数
    private Integer reply;//评论数
    private Integer danmaku;// 弹幕数
    private Integer favorite;//收藏数
    private Integer like;//点赞
    private Integer dislike;//踩
    private Integer share;// 分享数量
    private Integer coin;//推荐数量(硬币数)
    private String title;//标题
    private String desc;//简介
    private String pic;// 封面图片URL地址
    private String owner;// 投搞人
    private Integer cid;// 视频源及弹幕编号弹幕地址 http://comment.bilibili.cn/.xml
    private Long pubdate;//视频发布日期
    private Integer tid;//分区号
    private String tname;//分类名

    public static Info fromJson(int aid, String json) {
        Map map = gson.fromJson(json, Map.class);
        Map data = (Map) map.get("data");
        Map stat = (Map) ((Map) map.get("data")).get("stat");
        Info info = new Info();
        info.setAid(aid);
        info.setView(((Double) stat.get("view")).intValue());
        info.setDanmaku(((Double) stat.get("danmaku")).intValue());
        info.setReply(((Double) stat.get("reply")).intValue());
        info.setFavorite(((Double) stat.get("favorite")).intValue());
        info.setCoin(((Double) stat.get("coin")).intValue());
        info.setShare(((Double) stat.get("share")).intValue());
        info.setLike(((Double) stat.get("like")).intValue());
        info.setDislike(((Double) stat.get("dislike")).intValue());
        info.setDesc((String) data.get("desc"));
        info.setOwner((String) ((Map) data.get("owner")).get("name"));
        info.setPic((String) data.get("pic"));
        info.setPubdate(((Double) data.get("pubdate")).longValue());
        info.setTid(((Double) data.get("tid")).intValue());
        info.setCid(((Double) data.get("cid")).intValue());
        info.setTitle((String) data.get("title"));
        info.setTname((String) data.get("tname"));
        if (info.aid == null) {
            throw new RuntimeException("造型info失败");
        }
        return info;
    }

    public Put toPut() {
        Put put = new Put(Bytes.toBytes(getAid()));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("view"), Bytes.toBytes(view));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("reply"), Bytes.toBytes(reply));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("danmaku"), Bytes.toBytes(danmaku));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("favorite"), Bytes.toBytes(favorite));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("like"), Bytes.toBytes(like));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dislike"), Bytes.toBytes(dislike));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("share"), Bytes.toBytes(share));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("coin"), Bytes.toBytes(coin));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("title"), Bytes.toBytes(title));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("desc"), Bytes.toBytes(desc));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pic"), Bytes.toBytes(pic));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("owner"), Bytes.toBytes(owner));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cid"), Bytes.toBytes(cid));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pubdate"), Bytes.toBytes(pubdate));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("tid"), Bytes.toBytes(tid));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("tname"), Bytes.toBytes(tname));
        return put;
    }
}
