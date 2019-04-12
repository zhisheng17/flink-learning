import com.zhisheng.alert.model.AtMobiles;
import com.zhisheng.alert.model.LinkMessage;
import com.zhisheng.alert.model.MarkDownMessage;
import com.zhisheng.alert.model.TextMessage;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.common.utils.HttpUtil;

import java.util.Arrays;

/**
 * test
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class MessageTest {
    public static void main(String[] args) throws Exception {

        AtMobiles atMobiles = new AtMobiles();
        atMobiles.setIsAtAll(false);
        atMobiles.setAtMobiles(Arrays.asList("xxx"));

        TextMessage.TextContent content = new TextMessage.TextContent();
        content.setContent("Test text\nzhisheng");

        TextMessage text = new TextMessage(content, atMobiles);

        System.out.println(GsonUtil.toJson(text));


        LinkMessage.Link link = new LinkMessage.Link();
        link.setMessageUrl("https://github.com/");
        link.setPicUrl("https://ws1.sinaimg.cn/large/006tNc79ly1fz1ts9827yj30zk0kj46e.jpg");
        link.setText("Test link\nzhisheng");
        link.setTitle("Test link");
        LinkMessage linkMessage = new LinkMessage(link);

        System.out.println(GsonUtil.toJson(linkMessage));


        MarkDownMessage.MarkDownContent content1 = new MarkDownMessage.MarkDownContent();
        content1.setText("### Test markdown\n## zhisheng\n![](https://ws1.sinaimg.cn/large/006tNc79ly1fz1ts9827yj30zk0kj46e.jpg)");
        content1.setTitle("Test markdown");
        MarkDownMessage markDownMessage = new MarkDownMessage(content1, atMobiles);

        System.out.println(GsonUtil.toJson(markDownMessage));

        String dingdingUrl = "https://oapi.dingtalk.com/robot/send?access_token=a582138a7ab92cbda1ff59e1dcde67e6c50c33977d82fecb35e3c8bd7124c95c";
        HttpUtil.doPostString(dingdingUrl, GsonUtil.toJson(text));
        HttpUtil.doPostString(dingdingUrl, GsonUtil.toJson(linkMessage));
        HttpUtil.doPostString(dingdingUrl, GsonUtil.toJson(markDownMessage));
    }
}
