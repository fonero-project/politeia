// Copyright (c) 2017-2019 The Fonero developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package testpoliteiad

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	fonero "github.com/fonero-project/politeia/foneroplugin"
	v1 "github.com/fonero-project/politeia/politeiad/api/v1"
)

const (
	bestBlock uint32 = 1000
)

func (p *TestPoliteiad) authorizeVote(payload string) (string, error) {
	av, err := fonero.DecodeAuthorizeVote([]byte(payload))
	if err != nil {
		return "", err
	}

	// Sign authorize vote
	s := p.identity.SignMessage([]byte(av.Signature))
	av.Receipt = hex.EncodeToString(s[:])
	av.Timestamp = time.Now().Unix()
	av.Version = fonero.VersionAuthorizeVote

	p.Lock()
	defer p.Unlock()

	// Store authorize vote
	_, ok := p.authorizeVotes[av.Token]
	if !ok {
		p.authorizeVotes[av.Token] = make(map[string]fonero.AuthorizeVote)
	}

	r, err := p.record(av.Token)
	if err != nil {
		return "", err
	}

	p.authorizeVotes[av.Token][r.Version] = *av

	// Prepare reply
	avrb, err := fonero.EncodeAuthorizeVoteReply(
		fonero.AuthorizeVoteReply{
			Action:        av.Action,
			RecordVersion: r.Version,
			Receipt:       av.Receipt,
			Timestamp:     av.Timestamp,
		})
	if err != nil {
		return "", err
	}

	return string(avrb), nil
}

func (p *TestPoliteiad) startVote(payload string) (string, error) {
	sv, err := fonero.DecodeStartVote([]byte(payload))
	if err != nil {
		return "", err
	}

	p.Lock()
	defer p.Unlock()

	// Store start vote
	p.startVotes[sv.Vote.Token] = *sv

	// Prepare reply
	endHeight := bestBlock + sv.Vote.Duration
	svr := fonero.StartVoteReply{
		Version:          fonero.VersionStartVoteReply,
		StartBlockHeight: strconv.FormatUint(uint64(bestBlock), 10),
		EndHeight:        strconv.FormatUint(uint64(endHeight), 10),
		EligibleTickets:  []string{},
	}
	svrb, err := fonero.EncodeStartVoteReply(svr)
	if err != nil {
		return "", err
	}

	// Store reply
	p.startVoteReplies[sv.Vote.Token] = svr

	return string(svrb), nil
}

// foneroExec executes the passed in plugin command.
func (p *TestPoliteiad) foneroExec(pc v1.PluginCommand) (string, error) {
	switch pc.Command {
	case fonero.CmdStartVote:
		return p.startVote(pc.Payload)
	case fonero.CmdAuthorizeVote:
		return p.authorizeVote(pc.Payload)
	}
	return "", fmt.Errorf("invalid plugin command")
}
