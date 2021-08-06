// Copyright 2021 fangyousong(方友松). All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/clientv3"
)

const (
	ETCD_BASE           = "pretty-q"
	ETCD_TOPIC          = "topic"
	ETCD_SHARDING       = "sharding"
	ETCD_REPLICA_SET    = "replica-set"
	ETCD_REPLICA_LEADER = "leader"
	ETCD_REPLICA        = "replica"
)

// 考虑到多个prettyQ实例使用同一个etcd服务，因此通过instance来区分不同的prettyQ集群
type EtcdInstance struct {
	Name         string   //prettyQ instance name
	TopicList    []string //topic list
	ShardingList []string //sharding list
}

type EtcdTopic struct {
	Name     string //topic name
	FileSize uint32 //datafile max-size
}

type EtcdReplica struct {
	Name  string
	Addr  string
	MsgSn uint64 //递增全局消息id，用于协调同步副本间的状态一致性
}

func EtcdKeyTopicAll(prettyQInstance string) string {
	return fmt.Sprintf("%s/%s/%s", ETCD_BASE, prettyQInstance, ETCD_TOPIC)
}

func EtcdKeyTopic(prettyQInstance string, topic string) string {
	return fmt.Sprintf("%s/%s/%s/%s", ETCD_BASE, prettyQInstance, ETCD_TOPIC, topic)
}

func EtcdKeyShardingAll(prettyQInstance string) string {
	return fmt.Sprintf("%s/%s/%s", ETCD_BASE, prettyQInstance, ETCD_SHARDING)
}

func EtcdKeyReplicaSet(prettyQInstance string, sharding string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", ETCD_BASE, prettyQInstance, ETCD_SHARDING, sharding, ETCD_REPLICA_SET)
}

func EtcdKeyReplicaLeader(prettyQInstance string, sharding string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", ETCD_BASE, prettyQInstance, ETCD_SHARDING, sharding, ETCD_REPLICA_LEADER)
}

func EtcdKeyReplica(prettyQInstance string, sharding string, replica string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s", ETCD_BASE, prettyQInstance, ETCD_SHARDING, sharding, ETCD_REPLICA, replica)
}

// etcd客户端，用于查询和更新etcd中pretty-q的元数据，以及实现replica set leader选举等
type EtcdClient struct {
	instance string
	client   *clientv3.Client
	closed   uint32
}

func NewEtcdClient(prettyQInstance string, endpoints string, dialTimeout time.Duration) (*EtcdClient, error) {
	ret := &EtcdClient{instance: prettyQInstance}
	var err error
	ret.client, err = clientv3.New(clientv3.Config{Endpoints: strings.Split(endpoints, ","), DialTimeout: dialTimeout})
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (m *EtcdClient) Close() {
	if !atomic.CompareAndSwapUint32(&m.closed, 0, 1) {
		return
	}
	m.client.Close()
}

// 获取所有的sharding server信息，其中server地址为副本集中leader的地址
func (m *EtcdClient) GetShardings(timeout time.Duration) ([]*EtcdReplica, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := m.client.Get(ctx, EtcdKeyShardingAll(m.instance))
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("no sharding")
	}
	shardingList := strings.Split(resp.Kvs[0].String(), ",")
	ret := make([]*EtcdReplica, 0, len(shardingList))
	for _, v := range shardingList {
		// get leader replica name
		resp, err := m.client.Get(ctx, EtcdKeyReplicaLeader(m.instance, v))
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			return nil, fmt.Errorf("no replica leader of sharding %s", v)
		}
		leader := resp.Kvs[0].String()

		// get leader replica info
		resp, err = m.client.Get(ctx, EtcdKeyReplica(m.instance, v, leader))
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			return nil, fmt.Errorf("get replica leader %s of sharding %s, empty", leader, v)
		}
		replica := &EtcdReplica{}
		if err := json.Unmarshal(resp.Kvs[0].Value, replica); err != nil {
			return nil, fmt.Errorf("unmarshal replica %s fail, %s", leader, err.Error())
		}
		ret = append(ret, replica)
	}

	return ret, nil
}

// 获取所有的topic信息
func (m *EtcdClient) GetTopics(timeout time.Duration) ([]*EtcdTopic, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := m.client.Get(ctx, EtcdKeyTopicAll(m.instance))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("no topic")
	}

	topicList := strings.Split(resp.Kvs[0].String(), ",")
	ret := make([]*EtcdTopic, 0, len(topicList))
	for _, v := range topicList {
		resp, err := m.client.Get(ctx, EtcdKeyTopic(m.instance, v))
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			return nil, fmt.Errorf("get topic %s fail, empty", v)
		}

		topic := &EtcdTopic{}
		if err := json.Unmarshal(resp.Kvs[0].Value, topic); err != nil {
			return nil, fmt.Errorf("unmarshal topic %s fail, %s", v, err.Error())
		}
		ret = append(ret, topic)
	}

	return ret, nil
}

// 对某个sharding的副本集进行leader选举，抢占式先到先得
func (m *EtcdClient) ElectReplicaLeader(sharding string, previousLeader string, voteLeader string, timeout time.Duration) (success bool, newLeader string, e error) {
	if sharding == "" || previousLeader == "" || voteLeader == "" || previousLeader == voteLeader {
		return false, "", fmt.Errorf(`sharding == "" || previousLeader == "" || voteLeader == "" || previousLeader == voteLeader`)
	}
	kvc := clientv3.NewKV(m.client)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	leaderKey := EtcdKeyReplicaLeader(m.instance, sharding)
	_, err := kvc.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(leaderKey), "=", previousLeader)).
		Then(clientv3.OpPut(leaderKey, voteLeader)).
		Commit()
	if err != nil {
		return false, "", err
	}
	resp, err := kvc.Get(ctx, leaderKey)
	if err != nil {
		return false, "", fmt.Errorf("leader get fail, %s", err.Error())
	}
	if len(resp.Kvs) == 0 {
		return false, "", fmt.Errorf("leader get fail, empty")
	}
	if resp.Kvs[0].String() == voteLeader {
		return true, voteLeader, nil
	} else {
		return false, resp.Kvs[0].String(), nil
	}
}
