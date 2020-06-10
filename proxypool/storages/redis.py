import redis
from proxypool.exceptions import PoolEmptyException
from proxypool.schemas.proxy import Proxy
from proxypool.setting import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_KEY, PROXY_SCORE_MAX, PROXY_SCORE_MIN, \
    PROXY_SCORE_INIT
from random import choice
from typing import List
from loguru import logger
from proxypool.utils.proxy import is_valid_proxy, convert_proxy_or_proxies


REDIS_CLIENT_VERSION = redis.__version__
IS_REDIS_VERSION_2 = REDIS_CLIENT_VERSION.startswith('2.')


class RedisClient(object):
    """
    redis connection client of proxypool
    RedisClient 这个类可以用来操作 Redis 的有序集合
    """

    # 初始化了一个 StrictRedis 的类，建立 Redis 连接，传入常量信息(定义在setting.py中)
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, **kwargs):
        """
        init redis client
        :param host: redis host
        :param port: redis port
        :param password: redis password
        """
        self.db = redis.StrictRedis(host=host, port=port, password=password, decode_responses=True, **kwargs)

    # add 方法向数据库添加代理并设置分数，默认的分数是 PROXY_SCORE_INIT 也就是 10，返回结果是添加的结果
    def add(self, proxy: Proxy, score=PROXY_SCORE_INIT) -> int:
        """
        add proxy and set it to init score
        :param proxy: proxy, ip:port, like 8.8.8.8:88
        :param score: int score
        :return: result
        """
        if not is_valid_proxy(f'{proxy.host}:{proxy.port}'):
            logger.info(f'invalid proxy {proxy}, throw it')
            return
        if not self.exists(proxy):
            if IS_REDIS_VERSION_2:
                return self.db.zadd(REDIS_KEY, score, proxy.string())
            return self.db.zadd(REDIS_KEY, {proxy.string(): score})

    # random 方法是随机获取代理的方法，首先获取 100 分的代理，然后随机选择一个返回。
    # 如果不存在 100 分的代理，则此方法按照排名来获取，选取前 100 名，然后随机选择一个返回，否则抛出异常。
    def random(self) -> Proxy:
        """
        get random proxy
        firstly try to get proxy with max score
        if not exists, try to get proxy by rank
        if not exists, raise error
        :return: proxy, like 8.8.8.8:8
        """
        # try to get proxy with max score
        proxies = self.db.zrangebyscore(REDIS_KEY, PROXY_SCORE_MAX, PROXY_SCORE_MAX)
        if len(proxies):
            return convert_proxy_or_proxies(choice(proxies))
        # else get proxy by rank
        proxies = self.db.zrevrange(REDIS_KEY, PROXY_SCORE_MIN, PROXY_SCORE_MAX)
        if len(proxies):
            return convert_proxy_or_proxies(choice(proxies))
        # else raise error
        raise PoolEmptyException

    # decrease 方法是在代理检测无效的时候设置分数减 1 的方法，代理传入后，此方法将代理的分数减 1，如果分数达到最低值，那么代理就删除。
    def decrease(self, proxy: Proxy) -> int:
        """
        decrease score of proxy, if small than PROXY_SCORE_MIN, delete it
        :param proxy: proxy
        :return: new score
        """
        score = self.db.zscore(REDIS_KEY, proxy.string())
        # current score is larger than PROXY_SCORE_MIN
        if score and score > PROXY_SCORE_MIN:
            logger.info(f'{proxy.string()} current score {score}, decrease 1')
            if IS_REDIS_VERSION_2:
                return self.db.zincrby(REDIS_KEY, proxy.string(), -1)
            return self.db.zincrby(REDIS_KEY, -1, proxy.string())
        # otherwise delete proxy
        else:
            logger.info(f'{proxy.string()} current score {score}, remove')
            return self.db.zrem(REDIS_KEY, proxy.string())

    # exists 方法可判断代理是否存在集合中。
    def exists(self, proxy: Proxy) -> bool:
        """
        if proxy exists
        :param proxy: proxy
        :return: if exists, bool
        """
        return not self.db.zscore(REDIS_KEY, proxy.string()) is None

    # max 方法将代理的分数设置为 PROXY_SCORE_MAX，即 100，也就是当代理有效时的设置。
    def max(self, proxy: Proxy) -> int:
        """
        set proxy to max score
        :param proxy: proxy
        :return: new score
        """
        logger.info(f'{proxy.string()} is valid, set to {PROXY_SCORE_MAX}')
        if IS_REDIS_VERSION_2:
            return self.db.zadd(REDIS_KEY, PROXY_SCORE_MAX, proxy.string())
        return self.db.zadd(REDIS_KEY, {proxy.string(): PROXY_SCORE_MAX})

    # count 方法返回当前集合的元素个数
    def count(self) -> int:
        """
        get count of proxies
        :return: count, int
        """
        return self.db.zcard(REDIS_KEY)

    # all 方法返回所有的代理列表，供检测使用
    def all(self) -> List[Proxy]:
        """
        get all proxies
        :return: list of proxies
        """
        return convert_proxy_or_proxies(self.db.zrangebyscore(REDIS_KEY, PROXY_SCORE_MIN, PROXY_SCORE_MAX))

    # batch 方法返回一批代理
    def batch(self, start, end) -> List[Proxy]:
        """
        get batch of proxies
        :param start: start index
        :param end: end index
        :return: list of proxies
        """
        return convert_proxy_or_proxies(self.db.zrevrange(REDIS_KEY, start, end - 1))


if __name__ == '__main__':
    conn = RedisClient()
    result = conn.random()
    print(result)
