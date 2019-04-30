import redis.clients.jedis.Jedis;

/**
 * Desc: 验证数据已经写入到 Redis
 * Created by zhisheng on 2019-04-29
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class RedisTest {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("127.0.0.1");
        System.out.println("Server is running: " + jedis.ping());
        System.out.println("result:" + jedis.hgetAll("zhisheng"));
    }
}
