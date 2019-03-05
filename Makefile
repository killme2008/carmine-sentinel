THIS_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

sentinel.conf:
	cp raw-sentinel.conf $@
	echo 'requirepass "foobar"' >> $@
	echo 'daemonize yes' >> $@

sentinel.%.conf: sentinel.conf
	cp sentinel.conf sentinel.$*.conf
	echo pidfile $(THIS_DIR)/sentinel.$*.pid >> $@

sentinel.%: sentinel.%.conf
	redis-sentinel sentinel.$*.conf --port 500$* &

sentinels: sentinel.conf
	$(MAKE) sentinel.0
	$(MAKE) sentinel.1
	$(MAKE) sentinel.2

redis.conf:
	cp raw-redis.conf $@
	echo 'requirepass "foobar"' >> $@
	echo 'daemonize yes' >> $@
	echo pidfile $(THIS_DIR)/redis.pid >> $@

redis: redis.conf
	redis-server redis.conf &

test_setup: sentinels redis

test: test_setup
	lein test
	$(MAKE) stop
	$(MAKE) clean

clean:
	rm sentinel*.conf redis*.conf

stop:
	kill `cat sentinel.*.pid`
	kill `cat redis.pid`

.PRECIOUS: sentinel.%.conf redis.conf

.PHONY: clean stop test
