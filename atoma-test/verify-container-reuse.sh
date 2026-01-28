#!/bin/bash

echo "=== 验证 TestContainers 容器复用机制 ==="
echo

# 清理可能存在的容器
echo "1. 清理已存在的测试容器..."
docker ps -a | grep "atoma-test-mongodb" | awk '{print $1}' | xargs -r docker rm -f

# 设置环境变量
export TESTCONTAINERS_REUSE_ENABLE=true

echo
echo "2. 运行第一次测试..."
cd /home/maxuefeng/atoma/atoma-test
mvn test -Dtest=SimpleMutexTest -q

echo
echo "3. 检查容器状态..."
docker ps | grep "atoma-test-mongodb" || echo "警告: 未找到运行的容器"

echo
echo "4. 运行第二次测试..."
mvn test -Dtest=SingleClientLockAcquisitionTest -q

echo
echo "5. 验证容器是否复用..."
CONTAINER_ID=$(docker ps | grep "atoma-test-mongodb" | awk '{print $1}')
if [ -n "$CONTAINER_ID" ]; then
    echo "✓ 容器正在运行: $CONTAINER_ID"
    echo "✓ 容器创建时间: $(docker inspect -f '{{.Created}}' $CONTAINER_ID)"
else
    echo "✗ 容器未运行，复用机制可能未生效"
fi

echo
echo "6. 检查容器标签..."
docker inspect $CONTAINER_ID | grep -A 5 -B 5 "testcontainers.reuse" || echo "未找到复用标签"

echo
echo "=== 验证完成 ==="