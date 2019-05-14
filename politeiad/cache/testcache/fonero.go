// Copyright (c) 2017-2019 The Fonero developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package testcache

import (
	fonero "github.com/fonero-project/politeia/foneroplugin"
	"github.com/fonero-project/politeia/politeiad/cache"
)

func (c *testcache) getComments(payload string) (string, error) {
	gc, err := fonero.DecodeGetComments([]byte(payload))
	if err != nil {
		return "", err
	}

	c.RLock()
	defer c.RUnlock()

	gcrb, err := fonero.EncodeGetCommentsReply(
		fonero.GetCommentsReply{
			Comments: c.comments[gc.Token],
		})
	if err != nil {
		return "", err
	}

	return string(gcrb), nil
}

func (c *testcache) authorizeVote(cmdPayload, replyPayload string) (string, error) {
	av, err := fonero.DecodeAuthorizeVote([]byte(cmdPayload))
	if err != nil {
		return "", err
	}

	avr, err := fonero.DecodeAuthorizeVoteReply([]byte(replyPayload))
	if err != nil {
		return "", err
	}

	av.Receipt = avr.Receipt
	av.Timestamp = avr.Timestamp

	c.Lock()
	defer c.Unlock()

	_, ok := c.authorizeVotes[av.Token]
	if !ok {
		c.authorizeVotes[av.Token] = make(map[string]fonero.AuthorizeVote)
	}

	c.authorizeVotes[av.Token][avr.RecordVersion] = *av

	return replyPayload, nil
}

func (c *testcache) startVote(cmdPayload, replyPayload string) (string, error) {
	sv, err := fonero.DecodeStartVote([]byte(cmdPayload))
	if err != nil {
		return "", err
	}

	svr, err := fonero.DecodeStartVoteReply([]byte(replyPayload))
	if err != nil {
		return "", err
	}

	c.Lock()
	defer c.Unlock()

	// Store start vote data
	c.startVotes[sv.Vote.Token] = *sv
	c.startVoteReplies[sv.Vote.Token] = *svr

	return replyPayload, nil
}

func (c *testcache) voteDetails(payload string) (string, error) {
	vd, err := fonero.DecodeVoteDetails([]byte(payload))
	if err != nil {
		return "", err
	}

	c.Lock()
	defer c.Unlock()

	// Lookup the latest record version
	r, err := c.record(vd.Token)
	if err != nil {
		return "", err
	}

	// Prepare reply
	_, ok := c.authorizeVotes[vd.Token]
	if !ok {
		c.authorizeVotes[vd.Token] = make(map[string]fonero.AuthorizeVote)
	}

	vdb, err := fonero.EncodeVoteDetailsReply(
		fonero.VoteDetailsReply{
			AuthorizeVote:  c.authorizeVotes[vd.Token][r.Version],
			StartVote:      c.startVotes[vd.Token],
			StartVoteReply: c.startVoteReplies[vd.Token],
		})
	if err != nil {
		return "", err
	}

	return string(vdb), nil
}

func (c *testcache) foneroExec(cmd, cmdPayload, replyPayload string) (string, error) {
	switch cmd {
	case fonero.CmdGetComments:
		return c.getComments(cmdPayload)
	case fonero.CmdAuthorizeVote:
		return c.authorizeVote(cmdPayload, replyPayload)
	case fonero.CmdStartVote:
		return c.startVote(cmdPayload, replyPayload)
	case fonero.CmdVoteDetails:
		return c.voteDetails(cmdPayload)
	}

	return "", cache.ErrInvalidPluginCmd
}
