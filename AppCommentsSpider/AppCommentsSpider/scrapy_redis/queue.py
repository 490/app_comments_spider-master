from scrapy.utils.reqser import request_to_dict, request_from_dict

from . import picklecompat


class Base(object):
    """Per-spider base queue class"""

    def __init__(self, server, spider, key, serializer=None):
        """Initialize per-spider redis queue.

        Parameters
        ----------
        server : StrictRedis
            Redis client instance.
        spider : Spider
            Scrapy spider instance.
        key: str
            Redis key where to put and get messages.
        serializer : object
            Serializer object with ``loads`` and ``dumps`` methods.

        """
        if serializer is None:
            # Backward compatibility.
            # TODO: deprecate pickle.
            serializer = picklecompat
        if not hasattr(serializer, 'loads'):
            raise TypeError("serializer does not implement 'loads' function: %r"
                            % serializer)
        if not hasattr(serializer, 'dumps'):
            raise TypeError("serializer '%s' does not implement 'dumps' function: %r"
                            % serializer)

        self.server = server
        self.spider = spider
        self.key = key % {'spider': spider.name}
        self.serializer = serializer

    """
    要把一个Request对象存储到数据库中，但数据库无法直接存储对象,
    通过_encode_request()将Request序列化转成字符串
    """
    def _encode_request(self, request):
        """Encode a request object"""
        obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    """
    做反序列化
    """
    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, self.spider)

    """
    在父类Base中，__len__()、push()和pop()三个方法都未实现，直接抛出NotImplementedError异常，
    因此这个类不能直接使用,必须要实现一个子类来重写这三个方法，因此不同子类会有不同的实现和不同的功能。
    """
    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        self.server.delete(self.key)


"""
定义FifoQueue、PriorityQueue、LifoQueue三个子类来继承Base类，
并重写__len__()、push()和pop()三个方法。
"""


class FifoQueue(Base):
    """Per-spider FIFO queue"""

    """
    server对象就是一个Redis连接对象，
    可以直接调用其操作Redis的方法:llen()、lpush()、rpop()等,对数据库进行操作
    lpush，rpop，左进右出。即先进先出（First Input First Output，FIFO）
    """
    def __len__(self):
        """Return the length of the queue"""
        return self.server.llen(self.key)

    def push(self, request):
        """Push a request"""
        self.server.lpush(self.key, self._encode_request(request))

    def pop(self, timeout=0):
        """Pop a request"""
        if timeout > 0:
            data = self.server.brpop(self.key, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.rpop(self.key)
        if data:
            return self._decode_request(data)


class PriorityQueue(Base):
    """Per-spider priority queue abstraction using redis' sorted set"""
    """
    是优先级队列，
    在pop函数里，zrange()取出集合的第一个元素，即最高优先级的Request，
    再调用zremrangebyrank()操作，将这个元素删除
        
    """
    def __len__(self):
        """Return the length of the queue"""
        return self.server.zcard(self.key)

    def push(self, request):
        """Push a request"""
        data = self._encode_request(request)
        score = -request.priority
        # We don't use zadd method as the order of arguments change depending on
        # whether the class is Redis or StrictRedis, and the option of using
        # kwargs only accepts strings, not bytes.
        self.server.execute_command('ZADD', self.key, score, data)

    def pop(self, timeout=0):
        """
        Pop a request
        timeout not support in this queue class
        """

        # use atomic range/remove using multi/exec
        pipe = self.server.pipeline()
        pipe.multi()
        pipe.zrange(self.key, 0, 0).zremrangebyrank(self.key, 0, 0)
        results, count = pipe.execute()
        if results:
            return self._decode_request(results[0])


class LifoQueue(Base):
    """Per-spider LIFO queue."""
    """
    lpush,lpop，左进左出。即后进先出（Last In First Out，LIFO）
    """
    def __len__(self):
        """Return the length of the stack"""
        return self.server.llen(self.key)

    def push(self, request):
        """Push a request"""
        self.server.lpush(self.key, self._encode_request(request))

    def pop(self, timeout=0):
        """Pop a request"""
        if timeout > 0:
            data = self.server.blpop(self.key, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.lpop(self.key)

        if data:
            return self._decode_request(data)


# TODO: Deprecate the use of these names.
SpiderQueue = FifoQueue
SpiderStack = LifoQueue
SpiderPriorityQueue = PriorityQueue
