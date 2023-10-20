package cn.shh.test.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.*;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.domain.geo.BoundingBox;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.data.redis.domain.geo.RadiusShape;

import java.awt.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootTest
class SpringDataRedisTest {
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Test
    public void testString() {
        ValueOperations<String, Object> stringOperate = redisTemplate.opsForValue();
        // 直接添加，并设置超时时间
        stringOperate.set("test:string:base", "v1", 30, TimeUnit.SECONDS);
        // key不存在时才添加，并设置超时时间
        stringOperate.setIfAbsent("test:string:absent", "v1", 30, TimeUnit.SECONDS);
        // key存在时才添加，并设置超时时间
        stringOperate.setIfPresent("test:string:present", "v1", 30, TimeUnit.SECONDS);
        // 一次性添加多个
        HashMap<String, Object> hashMap = new HashMap<>();
        hashMap.put("test:string:map:k1", "v1");
        hashMap.put("test:string:map:k2", "v2");
        hashMap.put("test:string:map:k3", "v3");
        stringOperate.multiSet(hashMap);
    }

    @Test
    public void testHash(){
        HashOperations<String, Object, Object> hashOperate = redisTemplate.opsForHash();
        hashOperate.put("test:hash:base", "k1", "v1");
        hashOperate.putIfAbsent("test:hash:absent", "k1", "v1");

        HashMap<String, Object> hashMap = new HashMap<>();
        hashMap.put("k1", "v1");
        hashMap.put("k2", "v2");
        hashMap.put("k3", "v3");
        hashOperate.putAll("test:hash:all", hashMap);

        hashOperate.multiGet("test:hash:all", Stream.of("k1", "k2", "k3").collect(Collectors.toList()))
                .stream().map(String::valueOf).collect(Collectors.toList()).forEach(System.out::println);

    }

    @Test
    public void testList(){
        ListOperations<String, Object> listOperations = redisTemplate.opsForList();
        listOperations.leftPush("test:list:left:pivot", "one", "two");
        listOperations.leftPushAll("test:list:left:all", Stream.of("two", "three", "four", "five")
                .collect(Collectors.toSet()));
        listOperations.range("test:list:left:all", 0, 3).stream().map(String::valueOf)
                .collect(Collectors.toList()).forEach(System.out::println);
    }

    @Test
    public void testSet(){
        SetOperations<String, Object> setOperations = redisTemplate.opsForSet();
        setOperations.add("test:set:k1", "one", "two", "three");
        setOperations.add("test:set:k2", "three", "four", "five");
        System.out.print("差集：");
        setOperations.difference("test:set:k1", "test:set:k2").stream().forEach(e -> {
            System.out.print(String.valueOf(e) + "\t");
        });
        System.out.print("\n交集：");
        setOperations.intersect("test:set:k1", "test:set:k2").stream().forEach(e -> {
            System.out.print(String.valueOf(e) + "\t");
        });
        System.out.print("\n并集：");
        setOperations.union("test:set:k1", "test:set:k2").stream().forEach(e -> {
            System.out.print(String.valueOf(e) + "\t");
        });
        // 获取指定键映射的多个元素
        System.out.print("\nmembers：");
        setOperations.members("test:set:k1").forEach(e -> {
            System.out.print(String.valueOf(e) + "\t");
        });
        System.out.print("\n键test:set:k1中是否存在元素three：");
        System.out.print(setOperations.isMember("test:set:k1", "three"));
    }

    @Test
    public void testSortedSet(){
        ZSetOperations<String, Object> zSetOperations = redisTemplate.opsForZSet();
        zSetOperations.add("test:zset:k1", "v1", 10d);

        zSetOperations.add("test:zset:k2", Stream.of(new DefaultTypedTuple<Object>("v1", 10d),
                new DefaultTypedTuple<Object>("v2", 20d), new DefaultTypedTuple<Object>("v3", 30d))
                .collect(Collectors.toSet()));

        zSetOperations.addIfAbsent("test:zset:absent:k1", "v1", 100d);
        // 获取指定元素的score值
        System.out.println("test:zset:k1 v1 score: " + zSetOperations.score("test:zset:k1", "v1"));
        // 获取指定元素的排名
        System.out.println("test:zset:k1 v1 排名: " + zSetOperations.rank("test:zset:k1", "v1"));
        // 获取指定key中元素的数量
        System.out.println("test:zset:k1中元素数量: " + zSetOperations.zCard("test:zset:k1"));
        // 获取score值在指定范围内的元素
        System.out.println("test:zset:k2中score值在指定范围内的元素: " + zSetOperations.count("test:zset:k2", 10, 20));
        // 根据score值排序，后获取排名在指定范围内的元素
        System.out.println("test:zset:k2排名在指定范围内的元素: " + zSetOperations.range("test:zset:k2", 0, 2));

        // 求差集
        System.out.print("\n差集：");
        zSetOperations.difference("test:zset:k1", "test:zset:k2").forEach(e -> {
            System.out.print(String.valueOf(e) + "\t");
        });
        // 求交集
        System.out.print("\n交集：");
        zSetOperations.intersect("test:zset:k1", "test:zset:k2").forEach(e -> {
            System.out.print(String.valueOf(e) + "\t");
        });
        // 求并集
        System.out.print("\n并集：");
        zSetOperations.union("test:zset:k1", "test:zset:k2").forEach(e -> {
            System.out.print(String.valueOf(e) + "\t");
        });
    }

    @Test
    public void testGeospatial(){
        GeoOperations<String, Object> geoOperations = redisTemplate.opsForGeo();
        geoOperations.add("test:geo:k1", new Point(13.361389, 38.115556), "a");
        geoOperations.add("test:geo:k1", new Point(15.087269, 37.502669), "b");
        geoOperations.add("test:geo:k2", new RedisGeoCommands.GeoLocation<Object>("a",
                new Point(13.361389, 38.115556)));
        geoOperations.add("test:geo:k2", new RedisGeoCommands.GeoLocation<Object>("b",
                new Point(15.087269, 37.502669)));

        // 获取指定名称的经纬度
        System.out.print("\nposition: ");
        geoOperations.position("test:geo:k1", "a", "b").forEach(e -> {
            System.out.print(e + "\t");
        });
        // 获取两个位置之间的距离（可指定单位:m/km/mi(英里)/ft(英尺)）
        System.out.print("\ndistance: ");
        Distance distance = geoOperations.distance("test:geo:k1", "a", "b");
        System.out.print(distance.getMetric() + "\t");

        System.out.print("\nradius: ");
        GeoResults<RedisGeoCommands.GeoLocation<Object>> geoResults = geoOperations.radius("test:geo:k1",
                new Circle(new Point(13.361389, 38.115556), 100));
        System.out.print(geoResults + "\t");

        // 以指定点为中心，以指定距离为半径画圆，获取圆中符合要求的位置
        GeoResults<RedisGeoCommands.GeoLocation<Object>> result = geoOperations.search("test:geo:k1",
                new Circle(new Point(13.361389, 38.115556), 100));
        System.out.println("\nresult: " + result);
        // 获取 距离指定成员指定距离的范围内 符合要求的位置
        GeoResults result2 = geoOperations.search("test:geo:k1", new GeoReference.GeoMemberReference("a"),
                new Distance(1000, RedisGeoCommands.DistanceUnit.KILOMETERS));
        System.out.println("result2: " + result2);
        GeoResults result3 = geoOperations.search("test:geo:k1", new GeoReference.GeoMemberReference("a"),
                new BoundingBox(100, 100, Metrics.KILOMETERS));
        System.out.println("result3: " + result3);
        GeoResults result4 = geoOperations.search("test:geo:k1", new GeoReference.GeoMemberReference("a"),
                new RadiusShape(new Distance(1000, RedisGeoCommands.DistanceUnit.KILOMETERS)),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance());
        System.out.println("result4: " + result4);
    }

    @Test
    public void testHyperLogLog(){
        HyperLogLogOperations<String, Object> hyperLogLog = redisTemplate.opsForHyperLogLog();
        hyperLogLog.add("test:hyperlog:k1", "one", "two", "three", "three", "four", "five");
        System.out.println("test:hyperlog:k1 size: " + hyperLogLog.size("test:hyperlog:k1"));

        hyperLogLog.add("test:hyperlog:k2", "1", "2", "3", "3", "4", "5", "6");
        System.out.println("test:hyperlog:k2 size: " + hyperLogLog.size("test:hyperlog:k2"));

        // 将多个HyperLog合并为一个
        hyperLogLog.union("test:hyperlog:k3", "test:hyperlog:k1", "test:hyperlog:k2");
        System.out.println("test:hyperlog:k3 size: " + hyperLogLog.size("test:hyperlog:k3"));

        // 删除一个HyperLog
        hyperLogLog.delete("test:hyperlog:k1");
        System.out.println("test:hyperlog:k1 size: " + hyperLogLog.size("test:hyperlog:k1"));
    }

    @Test
    public void testElse(){
        StreamOperations<String, Object, Object> streamOperations = redisTemplate.opsForStream();
        HashMap<String, Object> hashMap = new HashMap<>();
        hashMap.put("name", "zhangsan");
        hashMap.put("age", "18");
        streamOperations.add("test:stream:k1", hashMap);
        System.out.println("test:stream:k1 size: " + streamOperations.size("test:stream:k1"));
    }
}