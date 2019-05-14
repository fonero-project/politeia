// Copyright (c) 2017-2019 The Fonero developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/davecgh/go-spew/spew"
	"github.com/fonero-project/politeia/foneroplugin"
	pd "github.com/fonero-project/politeia/politeiad/api/v1"
	"github.com/fonero-project/politeia/politeiad/cache"
	"github.com/fonero-project/politeia/util"
)

// foneroGetComment sends the fonero plugin getcomment command to the cache and
// returns the specified comment.
func (p *politeiawww) foneroGetComment(token, commentID string) (*foneroplugin.Comment, error) {
	// Setup plugin command
	gc := foneroplugin.GetComment{
		Token:     token,
		CommentID: commentID,
	}

	payload, err := foneroplugin.EncodeGetComment(gc)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             foneroplugin.ID,
		Command:        foneroplugin.CmdGetComment,
		CommandPayload: string(payload),
	}

	// Get comment from the cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	gcr, err := foneroplugin.DecodeGetCommentReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return &gcr.Comment, nil
}

// foneroGetComments sends the fonero plugin getcomments command to the cache
// and returns all of the comments for the passed in proposal token.
func (p *politeiawww) foneroGetComments(token string) ([]foneroplugin.Comment, error) {
	// Setup plugin command
	gc := foneroplugin.GetComments{
		Token: token,
	}

	payload, err := foneroplugin.EncodeGetComments(gc)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             foneroplugin.ID,
		Command:        foneroplugin.CmdGetComments,
		CommandPayload: string(payload),
	}

	// Get comments from the cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, fmt.Errorf("PluginExec: %v", err)
	}

	gcr, err := foneroplugin.DecodeGetCommentsReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return gcr.Comments, nil
}

// foneroCommentLikes sends the fonero plugin commentlikes command to the cache
// and returns all of the comment likes for the passed in comment.
func (p *politeiawww) foneroCommentLikes(token, commentID string) ([]foneroplugin.LikeComment, error) {
	// Setup plugin command
	cl := foneroplugin.CommentLikes{
		Token:     token,
		CommentID: commentID,
	}

	payload, err := foneroplugin.EncodeCommentLikes(cl)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             foneroplugin.ID,
		Command:        foneroplugin.CmdCommentLikes,
		CommandPayload: string(payload),
	}

	// Get comment likes from cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	clr, err := foneroplugin.DecodeCommentLikesReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return clr.CommentLikes, nil
}

// foneroPropCommentLikes sends the fonero plugin proposalcommentslikes command
// to the cache and returns all of the comment likes for the passed in proposal
// token.
func (p *politeiawww) foneroPropCommentLikes(token string) ([]foneroplugin.LikeComment, error) {
	// Setup plugin command
	pcl := foneroplugin.GetProposalCommentsLikes{
		Token: token,
	}

	payload, err := foneroplugin.EncodeGetProposalCommentsLikes(pcl)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             foneroplugin.ID,
		Command:        foneroplugin.CmdProposalCommentsLikes,
		CommandPayload: string(payload),
	}

	// Get proposal comment likes from cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	rp := []byte(reply.Payload)
	pclr, err := foneroplugin.DecodeGetProposalCommentsLikesReply(rp)
	if err != nil {
		return nil, err
	}

	return pclr.CommentsLikes, nil
}

// foneroVoteDetails sends the fonero plugin votedetails command to the cache
// and returns the vote details for the passed in proposal.
func (p *politeiawww) foneroVoteDetails(token string) (*foneroplugin.VoteDetailsReply, error) {
	// Setup plugin command
	vd := foneroplugin.VoteDetails{
		Token: token,
	}

	payload, err := foneroplugin.EncodeVoteDetails(vd)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             foneroplugin.ID,
		Command:        foneroplugin.CmdVoteDetails,
		CommandPayload: string(payload),
	}

	// Get vote details from cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	vdr, err := foneroplugin.DecodeVoteDetailsReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return vdr, nil
}

// foneroProposalVotes sends the fonero plugin proposalvotes command to the
// cache and returns the vote results for the passed in proposal.
func (p *politeiawww) foneroProposalVotes(token string) (*foneroplugin.VoteResultsReply, error) {
	// Setup plugin command
	vr := foneroplugin.VoteResults{
		Token: token,
	}

	payload, err := foneroplugin.EncodeVoteResults(vr)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             foneroplugin.ID,
		Command:        foneroplugin.CmdProposalVotes,
		CommandPayload: string(payload),
	}

	// Get proposal votes from cache
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	vrr, err := foneroplugin.DecodeVoteResultsReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return vrr, nil
}

// foneroInventory sends the fonero plugin inventory command to the cache and
// returns the fonero plugin inventory.
func (p *politeiawww) foneroInventory() (*foneroplugin.InventoryReply, error) {
	// Setup plugin command
	i := foneroplugin.Inventory{}
	payload, err := foneroplugin.EncodeInventory(i)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             foneroplugin.ID,
		Command:        foneroplugin.CmdInventory,
		CommandPayload: string(payload),
	}

	// Get cache inventory
	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	ir, err := foneroplugin.DecodeInventoryReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return ir, nil
}

// foneroTokenInventory sends the fonero plugin tokeninventory command to the
// cache.
func (p *politeiawww) foneroTokenInventory(bestBlock uint64) (*foneroplugin.TokenInventoryReply, error) {
	payload, err := foneroplugin.EncodeTokenInventory(
		foneroplugin.TokenInventory{
			BestBlock: bestBlock,
		})
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             foneroplugin.ID,
		Command:        foneroplugin.CmdTokenInventory,
		CommandPayload: string(payload),
	}

	reply, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	tir, err := foneroplugin.DecodeTokenInventoryReply([]byte(reply.Payload))
	if err != nil {
		return nil, err
	}

	return tir, nil
}

// foneroLoadVoteResults sends the loadvotesummaries command to politeiad.
func (p *politeiawww) foneroLoadVoteResults(bestBlock uint64) (*foneroplugin.LoadVoteResultsReply, error) {
	// Setup plugin command
	challenge, err := util.Random(pd.ChallengeSize)
	if err != nil {
		return nil, err
	}

	lvr := foneroplugin.LoadVoteResults{
		BestBlock: bestBlock,
	}
	payload, err := foneroplugin.EncodeLoadVoteResults(lvr)
	if err != nil {
		return nil, err
	}

	pc := pd.PluginCommand{
		Challenge: hex.EncodeToString(challenge),
		ID:        foneroplugin.ID,
		Command:   foneroplugin.CmdLoadVoteResults,
		CommandID: foneroplugin.CmdLoadVoteResults,
		Payload:   string(payload),
	}

	// Send plugin command to politeiad
	respBody, err := p.makeRequest(http.MethodPost,
		pd.PluginCommandRoute, pc)
	if err != nil {
		return nil, err
	}

	// Handle response
	var pcr pd.PluginCommandReply
	err = json.Unmarshal(respBody, &pcr)
	if err != nil {
		return nil, err
	}

	err = util.VerifyChallenge(p.cfg.Identity, challenge, pcr.Response)
	if err != nil {
		return nil, err
	}

	b := []byte(pcr.Payload)
	reply, err := foneroplugin.DecodeLoadVoteResultsReply(b)
	if err != nil {
		spew.Dump("here")
		return nil, err
	}

	return reply, nil
}

// foneroVoteSummary uses the fonero plugin vote summary command to request a
// vote summary for a specific proposal from the cache.
func (p *politeiawww) foneroVoteSummary(token string) (*foneroplugin.VoteSummaryReply, error) {
	v := foneroplugin.VoteSummary{
		Token: token,
	}
	payload, err := foneroplugin.EncodeVoteSummary(v)
	if err != nil {
		return nil, err
	}

	pc := cache.PluginCommand{
		ID:             foneroplugin.ID,
		Command:        foneroplugin.CmdVoteSummary,
		CommandPayload: string(payload),
	}

	resp, err := p.cache.PluginExec(pc)
	if err != nil {
		return nil, err
	}

	reply, err := foneroplugin.DecodeVoteSummaryReply([]byte(resp.Payload))
	if err != nil {
		return nil, err
	}

	return reply, nil
}
