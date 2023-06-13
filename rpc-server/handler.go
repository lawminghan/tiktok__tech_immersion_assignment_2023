package main

import (
	"context"
	"fmt"
	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
	"strings"
	"time"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	if err := validateSendRequest(req); err != nil {
		return nil, err
	}

	message := &Message{
		Message:   req.Message.GetText(),
		Sender:    req.Message.GetSender(),
		Timestamp: time.Now().Unix(),
	}

	chatID, err := getChatID(req.Message.GetChat())
	if err != nil {
		return nil, err
	}

	err = rdb.SaveMessage(ctx, chatID, message)
	if err != nil {
		return nil, err
	}

	resp := &rpc.SendResponse{
		Code: 0,
		Msg:  "success",
	}
	return resp, nil
}

func getChatID(chat string) (string, error) {
	lowercase := strings.ToLower(chat)
	senders := strings.Split(lowercase, ":")
	if len(senders) != 2 {
		return "", fmt.Errorf("invalid Chat ID '%s', should be in the format of user1:user2", chat)
	}

	sender1, sender2 := senders[0], senders[1]
	// sort both sender in ascending order to form the chatID
	if strings.Compare(sender1, sender2) > 0 {
		return fmt.Sprintf("%s:%s", sender2, sender1), nil
	}
	return fmt.Sprintf("%s:%s", sender1, sender2), nil
}

func validateSendRequest(req *rpc.SendRequest) error {
	chatID := req.Message.GetChat()
	senders := strings.Split(chatID, ":")
	if len(senders) != 2 {
		return fmt.Errorf("invalid Chat ID '%s', should be in the format of user1:user2", chatID)
	}

	sender1, sender2 := senders[0], senders[1]
	sender := req.Message.GetSender()

	if sender != sender1 && sender != sender2 {
		return fmt.Errorf("sender '%s' not in the chat room", sender)
	}

	return nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	chatID, err := getChatID(req.GetChat())
	if err != nil {
		return nil, err
	}

	start := req.GetCursor()
	end := start + int64(req.GetLimit())

	messages, err := rdb.GetMessagesByChatID(ctx, chatID, start, end, req.GetReverse())
	if err != nil {
		return nil, err
	}

	respMessages := make([]*rpc.Message, 0)
	var counter int32
	var nextCursor int64
	hasMore := false

	for _, msg := range messages {
		if counter+1 > req.GetLimit() {
			// Having an extra value here means it has more data
			hasMore = true
			nextCursor = end
			break
		}

		temp := &rpc.Message{
			Chat:     req.GetChat(),
			Text:     msg.Message,
			Sender:   msg.Sender,
			SendTime: msg.Timestamp,
		}

		respMessages = append(respMessages, temp)
		counter++
	}

	resp := rpc.NewPullResponse()
	resp.Messages = respMessages
	resp.Code = 0
	resp.Msg = "success"
	resp.HasMore = &hasMore
	resp.NextCursor = &nextCursor

	return resp, nil

}
