package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	cli *redis.Client
}

func (c *RedisClient) InitClient(ctx context.Context, address, password string) error {
	r := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password, // no password
		DB:       0,        // default DB
	})

	// test connection
	if err := r.Ping(ctx).Err(); err != nil {
		return err
	}

	c.cli = r
	return nil
}

type Message struct {
	Sender    string `json:"sender"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

func (c *RedisClient) SaveMessage(ctx context.Context, chatID string, message *Message) error {
	text, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	member := &redis.Z{
		Score:  float64(message.Timestamp), // for sorting
		Member: text,
	}

	_, err = c.cli.ZAdd(ctx, chatID, *member).Result()
	if err != nil {
		return fmt.Errorf("failed to add message to sorted set: %w", err)
	}

	return nil
}

func (c *RedisClient) GetMessagesByChatID(ctx context.Context, chatID string, start, end int64, reverse bool) ([]*Message, error) {
	var rawMessages []string
	var messages []*Message

	var cmd *redis.StringSliceCmd
	if reverse {
		cmd = c.cli.ZRevRange(ctx, chatID, start, end)
	} else {
		cmd = c.cli.ZRange(ctx, chatID, start, end)
	}

	rawMessages, err := cmd.Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve messages from sorted set: %w", err)
	}

	for _, msg := range rawMessages {
		temp := &Message{}
		err := json.Unmarshal([]byte(msg), temp)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal message: %w", err)
		}
		messages = append(messages, temp)
	}

	return messages, nil
}
