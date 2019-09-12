import com.google.common.collect.Lists;
import com.zhisheng.alert.utils.DingDingGroupMsgUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Desc: 钉钉发送消息测试
 * Created by zhisheng on 2019-06-20
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class DingDingMsgTest {
    public static void main(String[] args) {

        //要 @ 人的列表
        List<String> tels = new ArrayList<>();
        tels.add("138xxxx6721");
        tels.add("139xxxx6821");

        //发送 markdown 消息
        String markdownMsg = "【宕机告警】机器 300秒内无消息,疑似发生宕机\n\n集群:zhisheng\n\nIP:[10.117.0.118](http://xxxx_filter_host_ip=10.117.0.118&x_timestamp=1561105441347)\n\n最近消息时间:2019-06-21 16:24:01  对应数据如下:\n\n内存使用率:90.00%\n\nLoad 5分钟为:0.8\n\nSwap 内存使用率:21.1%\n\nCPU 剩余:21.98%\n\nIP:[10.117.0.118](https://xxx=alert&x_filter_host_ip=10.117.0.118&x_timestamp=1561105441347)\n\n最近消息时间:2019-06-21 16:24:01  对应数据如下:\n\n内存使用率:90.00%\n\nLoad 5分钟为:0.8\n\nSwap 内存使用率:21.1%\n\nCPU 剩余:21.98%\n\nIP:[10.117.0.118](https://xxxfilter_host_ip=10.117.0.118&x_timestamp=1561105441347)\n\n最近消息时间:2019-06-21 16:24:01  对应数据如下:\n\n内存使用率:90.00%\n\nLoad 5分钟为:0.8\n\nSwap 内存使用率:21.1%\n\nCPU 剩余:21.98%\n\nIP:[10.117.0.118](https://xxx_filter_host_ip=10.117.0.118&x_timestamp=1561105441347)\n\n最近消息时间:2019-06-21 16:24:01  对应数据如下:\n\n内存使用率:90.00%\n\nLoad 5分钟为:0.8\n\nSwap 内存使用率:21.1%\n\nCPU 剩余:21.98%";
        String markdown = DingDingGroupMsgUtil.setMarkdownMessage(false, "markdown", markdownMsg, tels);
        String markdownResult = DingDingGroupMsgUtil.sendDingDingMsg("https://oapi.dingtalk.com/robot/send?access_token=32e9d99020641c46146174de1ea437fffb4122f957102078b3f1898009af7e40", markdown);
        System.out.println(markdownResult);


        //发送文本消息
        String textMsg = "zhisheng 的博客 text 消息";
        String text = DingDingGroupMsgUtil.setTextMessage(false, textMsg, new ArrayList<>());
        String textResult = DingDingGroupMsgUtil.sendDingDingMsg("https://oapi.dingtalk.com/robot/send?access_token=32e9d99020641c46146174de1ea437fffb4122f957102078b3f1898009af7e40", text);
        System.out.println(textResult);


        //发送 link 消息
        String linkMsg = "zhisheng 的博客 link 消息";
        String link = DingDingGroupMsgUtil.setLinkMessage("http://www.54tianzhisheng.cn/", "http://www.54tianzhisheng.cn/img/avatar.png", linkMsg, "link 消息测试");
        String linkResult = DingDingGroupMsgUtil.sendDingDingMsg("https://oapi.dingtalk.com/robot/send?access_token=32e9d99020641c46146174de1ea437fffb4122f957102078b3f1898009af7e40", link);
        System.out.println(linkResult);


        //同一个钉钉地址发送多条消息
        String text1 = "111111";
        String text2 = "222222";
        String text3 = "333333";
        String text4 = "444444";
        DingDingGroupMsgUtil.sendDingDingMsg("https://oapi.dingtalk.com/robot/send?access_token=32e9d99020641c46146174de1ea437fffb4122f957102078b3f1898009af7e40", Lists.newArrayList(text1, text2, text3, text4));


        //多个钉钉发送同一条消息
        String hook1 = "https://oapi.dingtalk.com/robot/send?access_token=32e9d99020641c46146174de1ea437fffb4122f957102078b3f1898009af7e40";
        String hook2 = "https://oapi.dingtalk.com/robot/send?access_token=32e9d99020641c46146174de1ea437fffb4122f957102078b3f1898009af7e40";
        String hook3 = "https://oapi.dingtalk.com/robot/send?access_token=32e9d99020641c46146174de1ea437fffb4122f957102078b3f1898009af7e40";
        String hook4 = "https://oapi.dingtalk.com/robot/send?access_token=32e9d99020641c46146174de1ea437fffb4122f957102078b3f1898009af7e40";
        String msg = "zhisheng 的博客";
        DingDingGroupMsgUtil.sendDingDingMsg(Lists.newArrayList(hook1, hook2, hook3, hook4), msg);


        //给多个钉钉群发送多条消息
        DingDingGroupMsgUtil.sendDingDingMsg(Lists.newArrayList(hook1, hook2, hook3, hook4), Lists.newArrayList(text1, text2, text3, text4));
    }
}
