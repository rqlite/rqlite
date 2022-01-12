package etcd

import (
	"context"
	"encoding/json"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Client struct {
	client    *clientv3.Client
	key       string
	leaderKey string
}

type Node struct {
	ID      string `json:"id,omitempty"`
	APIAddr string `json:"api_addr,omitempty"`
	Addr    string `json:"addr,omitempty"`
}

func New(key string) (*Client, error) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"localhost:2379"},
	})
	if err != nil {
		return nil, err
	}
	return &Client{
		client:    c,
		key:       key,
		leaderKey: fmt.Sprintf("rqlite/%s/leader", key),
	}, nil
}

func (c *Client) GetLeader() (id string, apiAddr string, addr string, ok bool, e error) {
	kv := clientv3.NewKV(c.client)
	resp, err := kv.Get(context.Background(), c.leaderKey)
	if err != nil {
		e = err
		return
	}
	if len(resp.Kvs) == 0 || resp.Kvs[0].Value == nil {
		ok = false
		return
	}

	n := Node{}
	if err := json.Unmarshal(resp.Kvs[0].Value, &n); err != nil {
		e = err
		return
	}
	return n.ID, n.APIAddr, n.Addr, true, nil
}

func (c *Client) InitializeLeader(id, apiAddr, addr string) (bool, error) {
	b, err := json.Marshal(Node{
		ID:      id,
		APIAddr: apiAddr,
		Addr:    addr,
	})
	if err != nil {
		return false, err
	}

	kv := clientv3.NewKV(c.client)
	resp, err := kv.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.Version(c.leaderKey), "=", 0)).
		Then(
			clientv3.OpPut(c.leaderKey, string(b))).Commit()
	if err != nil {
		return false, err
	}

	return resp.Succeeded, nil
}

func (c *Client) SetLeader(id, apiAddr, addr string) error {
	b, err := json.Marshal(Node{
		ID:      id,
		APIAddr: apiAddr,
		Addr:    addr,
	})
	if err != nil {
		return err
	}

	kv := clientv3.NewKV(c.client)
	_, err = kv.Put(context.Background(), c.leaderKey, string(b))
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Close() error {
	return c.client.Close()
}
