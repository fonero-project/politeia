// Copyright (c) 2017-2019 The Fonero developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package cockroachdb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/fonero-project/politeia/foneroplugin"
	pd "github.com/fonero-project/politeia/politeiad/api/v1"
	"github.com/fonero-project/politeia/politeiad/cache"
	"github.com/jinzhu/gorm"
)

const (
	// foneroVersion is the version of the cache implementation of
	// fonero plugin. This may differ from the foneroplugin package
	// version.
	foneroVersion = "1.1"

	// Fonero plugin table names
	tableComments          = "comments"
	tableCommentLikes      = "comment_likes"
	tableCastVotes         = "cast_votes"
	tableAuthorizeVotes    = "authorize_votes"
	tableVoteOptions       = "vote_options"
	tableStartVotes        = "start_votes"
	tableVoteOptionResults = "vote_option_results"
	tableVoteResults       = "vote_results"

	// Vote option IDs
	voteOptionIDApproved = "yes"
)

// fonero implements the PluginDriver interface.
type fonero struct {
	recordsdb *gorm.DB              // Database context
	version   string                // Version of fonero cache plugin
	settings  []cache.PluginSetting // Plugin settings
}

// newComment inserts a Comment record into the database.  This function has a
// database parameter so that it can be called inside of a transaction when
// required.
func (d *fonero) newComment(db *gorm.DB, c Comment) error {
	return db.Create(&c).Error
}

// cmdNewComment creates a Comment record using the passed in payloads and
// inserts it into the database.
func (d *fonero) cmdNewComment(cmdPayload, replyPayload string) (string, error) {
	log.Tracef("fonero cmdNewComment")

	nc, err := foneroplugin.DecodeNewComment([]byte(cmdPayload))
	if err != nil {
		return "", err
	}
	ncr, err := foneroplugin.DecodeNewCommentReply([]byte(replyPayload))
	if err != nil {
		return "", err
	}

	c := convertNewCommentFromFonero(*nc, *ncr)
	err = d.newComment(d.recordsdb, c)

	return replyPayload, err
}

// newLikeComment inserts a LikeComment record into the database.  This
// function has a database parameter so that it can be called inside of a
// transaction when required.
func (d *fonero) newLikeComment(db *gorm.DB, lc LikeComment) error {
	return db.Create(&lc).Error
}

// cmdLikeComment creates a LikeComment record using the passed in payloads
// and inserts it into the database.
func (d *fonero) cmdLikeComment(cmdPayload, replyPayload string) (string, error) {
	log.Tracef("fonero cmdLikeComment")

	dlc, err := foneroplugin.DecodeLikeComment([]byte(cmdPayload))
	if err != nil {
		return "", err
	}

	lc := convertLikeCommentFromFonero(*dlc)
	err = d.newLikeComment(d.recordsdb, lc)

	return replyPayload, err
}

// cmdCensorComment censors an existing comment.  A censored comment has its
// comment message removed and is marked as censored.
func (d *fonero) cmdCensorComment(cmdPayload, replyPayload string) (string, error) {
	log.Tracef("fonero cmdCensorComment")

	cc, err := foneroplugin.DecodeCensorComment([]byte(cmdPayload))
	if err != nil {
		return "", err
	}

	c := Comment{
		Key: cc.Token + cc.CommentID,
	}
	err = d.recordsdb.Model(&c).
		Updates(map[string]interface{}{
			"comment":  "",
			"censored": true,
		}).Error

	return replyPayload, err
}

// cmdGetComment retreives the passed in comment from the database.
func (d *fonero) cmdGetComment(payload string) (string, error) {
	log.Tracef("fonero cmdGetComment")

	gc, err := foneroplugin.DecodeGetComment([]byte(payload))
	if err != nil {
		return "", err
	}

	c := Comment{
		Key: gc.Token + gc.CommentID,
	}
	err = d.recordsdb.Find(&c).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			err = cache.ErrRecordNotFound
		}
		return "", err
	}

	gcr := foneroplugin.GetCommentReply{
		Comment: convertCommentToFonero(c),
	}
	gcrb, err := foneroplugin.EncodeGetCommentReply(gcr)
	if err != nil {
		return "", err
	}

	return string(gcrb), nil
}

// cmdGetComments returns all of the comments for the passed in record token.
func (d *fonero) cmdGetComments(payload string) (string, error) {
	log.Tracef("fonero cmdGetComments")

	gc, err := foneroplugin.DecodeGetComments([]byte(payload))
	if err != nil {
		return "", err
	}

	comments := make([]Comment, 0, 1024) // PNOOMA
	err = d.recordsdb.
		Where("token = ?", gc.Token).
		Find(&comments).
		Error
	if err != nil {
		return "", err
	}

	dpc := make([]foneroplugin.Comment, 0, len(comments))
	for _, c := range comments {
		dpc = append(dpc, convertCommentToFonero(c))
	}

	gcr := foneroplugin.GetCommentsReply{
		Comments: dpc,
	}
	gcrb, err := foneroplugin.EncodeGetCommentsReply(gcr)
	if err != nil {
		return "", err
	}

	return string(gcrb), nil
}

// cmdCommentLikes returns all of the comment likes for the passed in comment.
func (d *fonero) cmdCommentLikes(payload string) (string, error) {
	log.Tracef("fonero cmdCommentLikes")

	cl, err := foneroplugin.DecodeCommentLikes([]byte(payload))
	if err != nil {
		return "", err
	}

	likes := make([]LikeComment, 1024) // PNOOMA
	err = d.recordsdb.
		Where("token = ? AND comment_id = ?", cl.Token, cl.CommentID).
		Find(&likes).
		Error
	if err != nil {
		return "", err
	}

	lc := make([]foneroplugin.LikeComment, 0, len(likes))
	for _, v := range likes {
		lc = append(lc, convertLikeCommentToFonero(v))
	}

	clr := foneroplugin.CommentLikesReply{
		CommentLikes: lc,
	}
	clrb, err := foneroplugin.EncodeCommentLikesReply(clr)
	if err != nil {
		return "", err
	}

	return string(clrb), nil
}

// cmdProposalLikes returns all of the comment likes for all comments of the
// passed in record token.
func (d *fonero) cmdProposalCommentsLikes(payload string) (string, error) {
	log.Tracef("fonero cmdProposalCommentsLikes")

	cl, err := foneroplugin.DecodeGetProposalCommentsLikes([]byte(payload))
	if err != nil {
		return "", err
	}

	likes := make([]LikeComment, 0, 1024) // PNOOMA
	err = d.recordsdb.
		Where("token = ?", cl.Token).
		Find(&likes).
		Error
	if err != nil {
		return "", err
	}

	lc := make([]foneroplugin.LikeComment, 0, len(likes))
	for _, v := range likes {
		lc = append(lc, convertLikeCommentToFonero(v))
	}

	clr := foneroplugin.GetProposalCommentsLikesReply{
		CommentsLikes: lc,
	}
	clrb, err := foneroplugin.EncodeGetProposalCommentsLikesReply(clr)
	if err != nil {
		return "", err
	}

	return string(clrb), nil
}

// newAuthorizeVote creates an AuthorizeVote record and inserts it into the
// database.  If a previous AuthorizeVote record exists for the passed in
// proposal and version, it will be deleted before the new AuthorizeVote record
// is inserted.
//
// This function must be called within a transaction.
func (d *fonero) newAuthorizeVote(tx *gorm.DB, av AuthorizeVote) error {
	// Delete authorize vote if one exists for this version
	err := tx.Where("key = ?", av.Key).
		Delete(AuthorizeVote{}).
		Error
	if err != nil {
		return fmt.Errorf("delete authorize vote: %v", err)
	}

	// Add new authorize vote
	err = tx.Create(&av).Error
	if err != nil {
		return fmt.Errorf("create authorize vote: %v", err)
	}

	return nil
}

// cmdAuthorizeVote creates a AuthorizeVote record using the passed in payloads
// and inserts it into the database.
func (d *fonero) cmdAuthorizeVote(cmdPayload, replyPayload string) (string, error) {
	log.Tracef("fonero cmdAuthorizeVote")

	av, err := foneroplugin.DecodeAuthorizeVote([]byte(cmdPayload))
	if err != nil {
		return "", err
	}
	avr, err := foneroplugin.DecodeAuthorizeVoteReply([]byte(replyPayload))
	if err != nil {
		return "", err
	}

	v, err := strconv.ParseUint(avr.RecordVersion, 10, 64)
	if err != nil {
		return "", fmt.Errorf("parse version '%v' failed: %v",
			avr.RecordVersion, err)
	}

	// Run update in a transaction
	a := convertAuthorizeVoteFromFonero(*av, *avr, v)
	tx := d.recordsdb.Begin()
	err = d.newAuthorizeVote(tx, a)
	if err != nil {
		tx.Rollback()
		return "", fmt.Errorf("newAuthorizeVote: %v", err)
	}

	// Commit transaction
	err = tx.Commit().Error
	if err != nil {
		return "", fmt.Errorf("commit transaction: %v", err)
	}

	return replyPayload, nil
}

// newStartVote inserts a StartVote record into the database.  This function
// has a database parameter so that it can be called inside of a transaction
// when required.
func (d *fonero) newStartVote(db *gorm.DB, sv StartVote) error {
	return db.Create(&sv).Error
}

// cmdStartVote creates a StartVote record using the passed in payloads and
// inserts it into the database.
func (d *fonero) cmdStartVote(cmdPayload, replyPayload string) (string, error) {
	log.Tracef("fonero cmdStartVote")

	sv, err := foneroplugin.DecodeStartVote([]byte(cmdPayload))
	if err != nil {
		return "", err
	}
	svr, err := foneroplugin.DecodeStartVoteReply([]byte(replyPayload))
	if err != nil {
		return "", err
	}

	endHeight, err := strconv.ParseUint(svr.EndHeight, 10, 64)
	if err != nil {
		return "", fmt.Errorf("parse end height '%v': %v",
			svr.EndHeight, err)
	}

	s := convertStartVoteFromFonero(*sv, *svr, endHeight)
	err = d.newStartVote(d.recordsdb, s)
	if err != nil {
		return "", err
	}

	return replyPayload, nil
}

// cmdVoteDetails returns the AuthorizeVote and StartVote records for the
// passed in record token.
func (d *fonero) cmdVoteDetails(payload string) (string, error) {
	log.Tracef("fonero cmdVoteDetails")

	vd, err := foneroplugin.DecodeVoteDetails([]byte(payload))
	if err != nil {
		return "", nil
	}

	// Lookup the most recent version of the record
	var r Record
	err = d.recordsdb.
		Where("records.token = ?", vd.Token).
		Order("records.version desc").
		Limit(1).
		Find(&r).
		Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			err = cache.ErrRecordNotFound
		}
		return "", err
	}

	// Lookup authorize vote
	var av AuthorizeVote
	key := vd.Token + strconv.FormatUint(r.Version, 10)
	err = d.recordsdb.
		Where("key = ?", key).
		Find(&av).
		Error
	if err == gorm.ErrRecordNotFound {
		// An authorize vote may note exist. This is ok.
	} else if err != nil {
		return "", fmt.Errorf("authorize vote lookup failed: %v", err)
	}

	// Lookup start vote
	var sv StartVote
	err = d.recordsdb.
		Where("token = ?", vd.Token).
		Preload("Options").
		Find(&sv).
		Error
	if err == gorm.ErrRecordNotFound {
		// A start vote may note exist. This is ok.
	} else if err != nil {
		return "", fmt.Errorf("start vote lookup failed: %v", err)
	}

	// Prepare reply
	dav := convertAuthorizeVoteToFonero(av)
	dsv, dsvr := convertStartVoteToFonero(sv)
	vdr := foneroplugin.VoteDetailsReply{
		AuthorizeVote:  dav,
		StartVote:      dsv,
		StartVoteReply: dsvr,
	}
	vdrb, err := foneroplugin.EncodeVoteDetailsReply(vdr)
	if err != nil {
		return "", err
	}

	return string(vdrb), nil
}

// newCastVote inserts a CastVote record into the database.  This function has
// a database parameter so that it can be called inside of a transaction when
// required.
func (d *fonero) newCastVote(db *gorm.DB, cv CastVote) error {
	return db.Create(&cv).Error
}

// cmdNewBallot creates CastVote records using the passed in payloads and
// inserts them into the database.
func (d *fonero) cmdNewBallot(cmdPayload, replyPayload string) (string, error) {
	log.Tracef("fonero cmdNewBallot")

	b, err := foneroplugin.DecodeBallot([]byte(cmdPayload))
	if err != nil {
		return "", err
	}

	// Add votes to database
	tx := d.recordsdb.Begin()
	for _, v := range b.Votes {
		cv := convertCastVoteFromFonero(v)
		err = d.newCastVote(tx, cv)
		if err != nil {
			tx.Rollback()
			return "", err
		}
	}

	err = tx.Commit().Error
	if err != nil {
		return "", fmt.Errorf("commit transaction failed: %v", err)
	}

	return replyPayload, nil
}

// cmdProposalVotes returns the StartVote record and all CastVote records for
// the passed in record token.
func (d *fonero) cmdProposalVotes(payload string) (string, error) {
	log.Tracef("fonero cmdProposalVotes")

	vr, err := foneroplugin.DecodeVoteResults([]byte(payload))
	if err != nil {
		return "", err
	}

	// Lookup start vote
	var sv StartVote
	err = d.recordsdb.
		Where("token = ?", vr.Token).
		Preload("Options").
		Find(&sv).
		Error
	if err == gorm.ErrRecordNotFound {
		// A start vote may note exist if the voting period has not
		// been started yet. This is ok.
	} else if err != nil {
		return "", fmt.Errorf("start vote lookup failed: %v", err)
	}

	// Lookup all cast votes
	var cv []CastVote
	err = d.recordsdb.
		Where("token = ?", vr.Token).
		Find(&cv).
		Error
	if err == gorm.ErrRecordNotFound {
		// No cast votes may exist yet. This is ok.
	} else if err != nil {
		return "", fmt.Errorf("cast votes lookup failed: %v", err)
	}

	// Prepare reply
	dsv, _ := convertStartVoteToFonero(sv)
	dcv := make([]foneroplugin.CastVote, 0, len(cv))
	for _, v := range cv {
		dcv = append(dcv, convertCastVoteToFonero(v))
	}

	vrr := foneroplugin.VoteResultsReply{
		StartVote: dsv,
		CastVotes: dcv,
	}

	vrrb, err := foneroplugin.EncodeVoteResultsReply(vrr)
	if err != nil {
		return "", err
	}

	return string(vrrb), nil
}

// cmdInventory returns the fonero plugin inventory.
func (d *fonero) cmdInventory() (string, error) {
	log.Tracef("fonero cmdInventory")

	// XXX the only part of the fonero plugin inventory that we return
	// at the moment is comments. This is because comments are the only
	// thing politeiawww currently needs on startup.

	// Get all comments
	var c []Comment
	err := d.recordsdb.Find(&c).Error
	if err != nil {
		return "", err
	}

	dc := make([]foneroplugin.Comment, 0, len(c))
	for _, v := range c {
		dc = append(dc, convertCommentToFonero(v))
	}

	// Prepare inventory reply
	ir := foneroplugin.InventoryReply{
		Comments: dc,
	}
	irb, err := foneroplugin.EncodeInventoryReply(ir)
	if err != nil {
		return "", err
	}

	return string(irb), err
}

// newVoteResults creates a VoteResults record for a proposal and inserts it
// into the cache. A VoteResults record should only be created for proposals
// once the voting period has ended.
func (d *fonero) newVoteResults(token string) error {
	log.Tracef("newVoteResults %v", token)

	// Lookup start vote
	var sv StartVote
	err := d.recordsdb.
		Where("token = ?", token).
		Preload("Options").
		Find(&sv).
		Error
	if err != nil {
		return fmt.Errorf("lookup start vote: %v", err)
	}

	// Lookup cast votes
	var cv []CastVote
	err = d.recordsdb.
		Where("token = ?", token).
		Find(&cv).
		Error
	if err == gorm.ErrRecordNotFound {
		// No cast votes exists. In theory, this could
		// happen if no one were to vote on a proposal.
		// In practice, this shouldn't happen.
	} else if err != nil {
		return fmt.Errorf("lookup cast votes: %v", err)
	}

	// Tally cast votes
	tally := make(map[string]uint64) // [voteBit]voteCount
	for _, v := range cv {
		tally[v.VoteBit]++
	}

	// Create vote option results
	results := make([]VoteOptionResult, 0, len(sv.Options))
	for _, v := range sv.Options {
		voteBit := strconv.FormatUint(v.Bits, 16)
		voteCount := tally[voteBit]

		results = append(results, VoteOptionResult{
			Key:    token + voteBit,
			Votes:  voteCount,
			Option: v,
		})
	}

	// Check whether vote was approved
	var total uint64
	for _, v := range results {
		total += v.Votes
	}

	eligible := len(strings.Split(sv.EligibleTickets, ","))
	quorum := uint64(float64(sv.QuorumPercentage) / 100 * float64(eligible))
	pass := uint64(float64(sv.PassPercentage) / 100 * float64(total))

	// XXX: this only supports proposals with yes/no
	// voting options. Multiple voting option support
	// will need to be added in the future.
	var approvedVotes uint64
	for _, v := range results {
		if v.Option.ID == voteOptionIDApproved {
			approvedVotes = v.Votes
		}
	}

	var approved bool
	switch {
	case total < quorum:
		// Quorum not met
	case approvedVotes < pass:
		// Pass percentage not met
	default:
		// Vote was approved
		approved = true
	}

	// Create a vote results entry
	err = d.recordsdb.Create(&VoteResults{
		Token:    token,
		Approved: approved,
		Results:  results,
	}).Error
	if err != nil {
		return fmt.Errorf("new vote results: %v", err)
	}

	return nil
}

// cmdLoadVoteResults creates vote results entries for any proposals that have
// a finished voting period but have not yet been added to the vote results
// table. The vote results table is lazy loaded.
func (d *fonero) cmdLoadVoteResults(payload string) (string, error) {
	log.Tracef("cmdLoadVoteResults")

	lvs, err := foneroplugin.DecodeLoadVoteResults([]byte(payload))
	if err != nil {
		return "", err
	}

	// Find proposals that have a finished voting period but
	// have not yet been added to the vote results table.
	q := `SELECT start_votes.token
        FROM start_votes
        LEFT OUTER JOIN vote_results
          ON start_votes.token = vote_results.token
          WHERE start_votes.end_height <= ?
          AND vote_results.token IS NULL`
	rows, err := d.recordsdb.Raw(q, lvs.BestBlock).Rows()
	if err != nil {
		return "", fmt.Errorf("no vote results: %v", err)
	}
	defer rows.Close()

	var token string
	tokens := make([]string, 0, 1024)
	for rows.Next() {
		rows.Scan(&token)
		tokens = append(tokens, token)
	}

	// Create vote result entries
	for _, v := range tokens {
		err := d.newVoteResults(v)
		if err != nil {
			return "", fmt.Errorf("newVoteResults %v: %v", v, err)
		}
	}

	// Prepare reply
	r := foneroplugin.LoadVoteResultsReply{}
	reply, err := foneroplugin.EncodeLoadVoteResultsReply(r)
	if err != nil {
		return "", err
	}

	return string(reply), nil
}

// cmdTokenInventory returns the tokens of all records in the cache,
// categorized by stage of the voting process.
func (d *fonero) cmdTokenInventory(payload string) (string, error) {
	log.Tracef("fonero cmdTokenInventory")

	ti, err := foneroplugin.DecodeTokenInventory([]byte(payload))
	if err != nil {
		return "", err
	}

	// The token inventory call cannot be completed if there
	// are any proposals that have finished voting but that
	// don't have an entry in the vote results table yet.
	// Fail here if any are found.
	q := `SELECT start_votes.token
        FROM start_votes
        LEFT OUTER JOIN vote_results
          ON start_votes.token = vote_results.token
          WHERE start_votes.end_height <= ?
          AND vote_results.token IS NULL`
	rows, err := d.recordsdb.Raw(q, ti.BestBlock).Rows()
	if err != nil {
		return "", fmt.Errorf("no vote results: %v", err)
	}
	defer rows.Close()

	var token string
	missing := make([]string, 0, 1024)
	for rows.Next() {
		rows.Scan(&token)
		missing = append(missing, token)
	}

	if len(missing) > 0 {
		// Return a ErrRecordNotFound to indicate one
		// or more vote result records were not found.
		return "", cache.ErrRecordNotFound
	}

	// Pre voting period tokens. This query returns the
	// tokens of the most recent version of all records that
	// are public and do not have an associated StartVote
	// record, ordered by timestamp in descending order.
	q = `SELECT a.token
        FROM records a
        LEFT OUTER JOIN start_votes
          ON a.token = start_votes.token
        LEFT OUTER JOIN records b
          ON a.token = b.token
          AND a.version < b.version
        WHERE b.token IS NULL
          AND start_votes.token IS NULL
          AND a.status = ?
        ORDER BY a.timestamp DESC`
	rows, err = d.recordsdb.Raw(q, pd.RecordStatusPublic).Rows()
	if err != nil {
		return "", fmt.Errorf("pre: %v", err)
	}
	defer rows.Close()

	pre := make([]string, 0, 1024)
	for rows.Next() {
		rows.Scan(&token)
		pre = append(pre, token)
	}

	// Active voting period tokens
	q = `SELECT token
       FROM start_votes
       WHERE end_height > ?
       ORDER BY end_height DESC`
	rows, err = d.recordsdb.Raw(q, ti.BestBlock).Rows()
	if err != nil {
		return "", fmt.Errorf("active: %v", err)
	}
	defer rows.Close()

	active := make([]string, 0, 1024)
	for rows.Next() {
		rows.Scan(&token)
		active = append(active, token)
	}

	// Approved vote tokens
	q = `SELECT vote_results.token
       FROM vote_results
       INNER JOIN start_votes
         ON vote_results.token = start_votes.token
         WHERE vote_results.approved = true
       ORDER BY start_votes.end_height DESC`
	rows, err = d.recordsdb.Raw(q).Rows()
	if err != nil {
		return "", fmt.Errorf("approved: %v", err)
	}
	defer rows.Close()

	approved := make([]string, 0, 1024)
	for rows.Next() {
		rows.Scan(&token)
		approved = append(approved, token)
	}

	// Rejected vote tokens
	q = `SELECT vote_results.token
       FROM vote_results
       INNER JOIN start_votes
         ON vote_results.token = start_votes.token
         WHERE vote_results.approved = false
       ORDER BY start_votes.end_height DESC`
	rows, err = d.recordsdb.Raw(q).Rows()
	if err != nil {
		return "", fmt.Errorf("rejected: %v", err)
	}
	defer rows.Close()

	rejected := make([]string, 0, 1024)
	for rows.Next() {
		rows.Scan(&token)
		rejected = append(rejected, token)
	}

	// Abandoned tokens
	abandoned := make([]string, 0, 1024)
	q = `SELECT token
       FROM records
       WHERE status = ?
       ORDER BY timestamp DESC`
	rows, err = d.recordsdb.Raw(q, pd.RecordStatusArchived).Rows()
	if err != nil {
		return "", fmt.Errorf("abandoned: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(&token)
		abandoned = append(abandoned, token)
	}

	// Prepare reply
	reply, err := foneroplugin.EncodeTokenInventoryReply(
		foneroplugin.TokenInventoryReply{
			Pre:       pre,
			Active:    active,
			Approved:  approved,
			Rejected:  rejected,
			Abandoned: abandoned,
		})
	if err != nil {
		return "", err
	}

	return string(reply), nil
}

func (d *fonero) cmdVoteSummary(payload string) (string, error) {
	log.Tracef("cmdVoteSummary")

	vs, err := foneroplugin.DecodeVoteSummary([]byte(payload))
	if err != nil {
		return "", err
	}

	// Lookup the most recent record version
	var r Record
	err = d.recordsdb.
		Where("records.token = ?", vs.Token).
		Order("records.version desc").
		Limit(1).
		Find(&r).
		Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			err = cache.ErrRecordNotFound
		}
		return "", err
	}

	// Declare here to prevent goto errors
	results := make([]foneroplugin.VoteOptionResult, 0, 16)
	var (
		av AuthorizeVote
		sv StartVote
		vr VoteResults
	)

	// Lookup authorize vote
	key := vs.Token + strconv.FormatUint(r.Version, 10)
	err = d.recordsdb.
		Where("key = ?", key).
		Find(&av).
		Error
	if err == gorm.ErrRecordNotFound {
		// If an authorize vote doesn't exist
		// then there is no need to continue.
		goto sendReply
	} else if err != nil {
		return "", fmt.Errorf("lookup authorize vote: %v", err)
	}

	// Lookup start vote
	err = d.recordsdb.
		Where("token = ?", vs.Token).
		Preload("Options").
		Find(&sv).
		Error
	if err == gorm.ErrRecordNotFound {
		// If an start vote doesn't exist then
		// there is no need to continue.
		goto sendReply
	} else if err != nil {
		return "", fmt.Errorf("lookup start vote: %v", err)
	}

	// Lookup vote results
	err = d.recordsdb.
		Where("token = ?", vs.Token).
		Preload("Results").
		Preload("Results.Option").
		Find(&vr).
		Error
	if err == gorm.ErrRecordNotFound {
		// A vote results record was not found. This means that
		// the vote is either still active or has not been lazy
		// loaded yet. The vote results will need to be looked
		// up manually.
	} else if err != nil {
		return "", fmt.Errorf("lookup vote results: %v", err)
	} else {
		// Vote results record exists. We have all of the data
		// that we need to send the reply.
		vor := convertVoteOptionResultsToFonero(vr.Results)
		results = append(results, vor...)
		goto sendReply
	}

	// Lookup vote results manually
	for _, v := range sv.Options {
		var votes uint64
		tokenVoteBit := v.Token + strconv.FormatUint(v.Bits, 16)
		err := d.recordsdb.
			Model(&CastVote{}).
			Where("token_vote_bit = ?", tokenVoteBit).
			Count(&votes).
			Error
		if err != nil {
			return "", fmt.Errorf("count cast votes: %v", err)
		}

		results = append(results,
			foneroplugin.VoteOptionResult{
				ID:          v.ID,
				Description: v.Description,
				Bits:        v.Bits,
				Votes:       votes,
			})
	}

sendReply:
	// Return "" not "0" if end height doesn't exist
	var endHeight string
	if sv.EndHeight != 0 {
		endHeight = strconv.FormatUint(sv.EndHeight, 10)
	}

	vsr := foneroplugin.VoteSummaryReply{
		Authorized:          (av.Action == foneroplugin.AuthVoteActionAuthorize),
		EndHeight:           endHeight,
		EligibleTicketCount: sv.EligibleTicketCount,
		QuorumPercentage:    sv.QuorumPercentage,
		PassPercentage:      sv.PassPercentage,
		Results:             results,
	}
	reply, err := foneroplugin.EncodeVoteSummaryReply(vsr)
	if err != nil {
		return "", err
	}

	return string(reply), nil
}

// Exec executes a fonero plugin command.  Plugin commands that write data to
// the cache require both the command payload and the reply payload.  Plugin
// commands that fetch data from the cache require only the command payload.
// All commands return the appropriate reply payload.
func (d *fonero) Exec(cmd, cmdPayload, replyPayload string) (string, error) {
	log.Tracef("fonero Exec: %v", cmd)

	switch cmd {
	case foneroplugin.CmdAuthorizeVote:
		return d.cmdAuthorizeVote(cmdPayload, replyPayload)
	case foneroplugin.CmdStartVote:
		return d.cmdStartVote(cmdPayload, replyPayload)
	case foneroplugin.CmdVoteDetails:
		return d.cmdVoteDetails(cmdPayload)
	case foneroplugin.CmdBallot:
		return d.cmdNewBallot(cmdPayload, replyPayload)
	case foneroplugin.CmdBestBlock:
		return "", nil
	case foneroplugin.CmdNewComment:
		return d.cmdNewComment(cmdPayload, replyPayload)
	case foneroplugin.CmdLikeComment:
		return d.cmdLikeComment(cmdPayload, replyPayload)
	case foneroplugin.CmdCensorComment:
		return d.cmdCensorComment(cmdPayload, replyPayload)
	case foneroplugin.CmdGetComment:
		return d.cmdGetComment(cmdPayload)
	case foneroplugin.CmdGetComments:
		return d.cmdGetComments(cmdPayload)
	case foneroplugin.CmdProposalVotes:
		return d.cmdProposalVotes(cmdPayload)
	case foneroplugin.CmdCommentLikes:
		return d.cmdCommentLikes(cmdPayload)
	case foneroplugin.CmdProposalCommentsLikes:
		return d.cmdProposalCommentsLikes(cmdPayload)
	case foneroplugin.CmdInventory:
		return d.cmdInventory()
	case foneroplugin.CmdLoadVoteResults:
		return d.cmdLoadVoteResults(cmdPayload)
	case foneroplugin.CmdTokenInventory:
		return d.cmdTokenInventory(cmdPayload)
	case foneroplugin.CmdVoteSummary:
		return d.cmdVoteSummary(cmdPayload)
	}

	return "", cache.ErrInvalidPluginCmd
}

// createTables creates the cache tables needed by the fonero plugin if they do
// not already exist. A fonero plugin version record is inserted into the
// database during table creation.
//
// This function must be called within a transaction.
func (d *fonero) createTables(tx *gorm.DB) error {
	log.Tracef("createTables")

	// Create fonero plugin tables
	if !tx.HasTable(tableComments) {
		err := tx.CreateTable(&Comment{}).Error
		if err != nil {
			return err
		}
	}
	if !tx.HasTable(tableCommentLikes) {
		err := tx.CreateTable(&LikeComment{}).Error
		if err != nil {
			return err
		}
	}
	if !tx.HasTable(tableCastVotes) {
		err := tx.CreateTable(&CastVote{}).Error
		if err != nil {
			return err
		}
	}
	if !tx.HasTable(tableAuthorizeVotes) {
		err := tx.CreateTable(&AuthorizeVote{}).Error
		if err != nil {
			return err
		}
	}
	if !tx.HasTable(tableVoteOptions) {
		err := tx.CreateTable(&VoteOption{}).Error
		if err != nil {
			return err
		}
	}
	if !tx.HasTable(tableStartVotes) {
		err := tx.CreateTable(&StartVote{}).Error
		if err != nil {
			return err
		}
	}
	if !tx.HasTable(tableVoteOptionResults) {
		err := tx.CreateTable(&VoteOptionResult{}).Error
		if err != nil {
			return err
		}
	}
	if !tx.HasTable(tableVoteResults) {
		err := tx.CreateTable(&VoteResults{}).Error
		if err != nil {
			return err
		}
	}

	// Check if a fonero version record exists. Insert one
	// if no version record is found.
	if !tx.HasTable(tableVersions) {
		// This should never happen
		return fmt.Errorf("versions table not found")
	}

	var v Version
	err := tx.Where("id = ?", foneroplugin.ID).Find(&v).Error
	if err == gorm.ErrRecordNotFound {
		err = tx.Create(
			&Version{
				ID:        foneroplugin.ID,
				Version:   foneroVersion,
				Timestamp: time.Now().Unix(),
			}).Error
	}

	return err
}

// droptTables drops all fonero plugin tables from the cache and remove the
// fonero plugin version record.
//
// This function must be called within a transaction.
func (d *fonero) dropTables(tx *gorm.DB) error {
	// Drop fonero plugin tables
	err := tx.DropTableIfExists(tableComments, tableCommentLikes,
		tableCastVotes, tableAuthorizeVotes, tableVoteOptions,
		tableStartVotes, tableVoteOptionResults, tableVoteResults).
		Error
	if err != nil {
		return err
	}

	// Remove fonero plugin version record
	return tx.Delete(&Version{
		ID: foneroplugin.ID,
	}).Error
}

// build the fonero plugin cache using the passed in inventory.
//
// This function cannot be called using a transaction because it could
// potentially exceed cockroachdb's transaction size limit.
func (d *fonero) build(ir *foneroplugin.InventoryReply) error {
	log.Tracef("fonero build")

	// Drop all fonero plugin tables
	tx := d.recordsdb.Begin()
	err := d.dropTables(tx)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("drop tables: %v", err)
	}
	err = tx.Commit().Error
	if err != nil {
		return err
	}

	// Create fonero plugin tables
	tx = d.recordsdb.Begin()
	err = d.createTables(tx)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("create tables: %v", err)
	}
	err = tx.Commit().Error
	if err != nil {
		return err
	}

	// Build comments cache
	log.Tracef("fonero: building comments cache")
	for _, v := range ir.Comments {
		c := convertCommentFromFonero(v)
		err := d.newComment(d.recordsdb, c)
		if err != nil {
			log.Debugf("newComment failed on '%v'", c)
			return fmt.Errorf("newComment: %v", err)
		}
	}

	// Build like comments cache
	log.Tracef("fonero: building like comments cache")
	for _, v := range ir.LikeComments {
		lc := convertLikeCommentFromFonero(v)
		err := d.newLikeComment(d.recordsdb, lc)
		if err != nil {
			log.Debugf("newLikeComment failed on '%v'", lc)
			return fmt.Errorf("newLikeComment: %v", err)
		}
	}

	// Put authorize vote replies in a map for quick lookups
	avr := make(map[string]foneroplugin.AuthorizeVoteReply,
		len(ir.AuthorizeVoteReplies)) // [receipt]AuthorizeVote
	for _, v := range ir.AuthorizeVoteReplies {
		avr[v.Receipt] = v
	}

	// Build authorize vote cache
	log.Tracef("fonero: building authorize vote cache")
	for _, v := range ir.AuthorizeVotes {
		r, ok := avr[v.Receipt]
		if !ok {
			return fmt.Errorf("AuthorizeVoteReply not found %v",
				v.Token)
		}

		rv, err := strconv.ParseUint(r.RecordVersion, 10, 64)
		if err != nil {
			log.Debugf("newAuthorizeVote failed on '%v'", r)
			return fmt.Errorf("parse version '%v' failed: %v",
				r.RecordVersion, err)
		}

		av := convertAuthorizeVoteFromFonero(v, r, rv)
		err = d.newAuthorizeVote(d.recordsdb, av)
		if err != nil {
			log.Debugf("newAuthorizeVote failed on '%v'", av)
			return fmt.Errorf("newAuthorizeVote: %v", err)
		}
	}

	// Build start vote cache
	log.Tracef("fonero: building start vote cache")
	for _, v := range ir.StartVoteTuples {
		endHeight, err := strconv.ParseUint(v.StartVoteReply.EndHeight, 10, 64)
		if err != nil {
			log.Debugf("newStartVote failed on '%v'", v)
			return fmt.Errorf("parse end height '%v': %v",
				v.StartVoteReply.EndHeight, err)
		}

		sv := convertStartVoteFromFonero(v.StartVote,
			v.StartVoteReply, endHeight)
		err = d.newStartVote(d.recordsdb, sv)
		if err != nil {
			log.Debugf("newStartVote failed on '%v'", sv)
			return fmt.Errorf("newStartVote: %v", err)
		}
	}

	// Build cast vote cache
	log.Tracef("fonero: building cast vote cache")
	for _, v := range ir.CastVotes {
		cv := convertCastVoteFromFonero(v)
		err := d.newCastVote(d.recordsdb, cv)
		if err != nil {
			log.Debugf("newCastVote failed on '%v'", cv)
			return fmt.Errorf("newCastVote: %v", err)
		}
	}

	return nil
}

// Build drops all existing fonero plugin tables from the database, recreates
// them, then uses the passed in inventory payload to build the fonero plugin
// cache.
func (d *fonero) Build(payload string) error {
	log.Tracef("fonero Build")

	// Decode the payload
	ir, err := foneroplugin.DecodeInventoryReply([]byte(payload))
	if err != nil {
		return fmt.Errorf("DecodeInventoryReply: %v", err)
	}

	// Build the fonero plugin cache. This is not run using
	// a transaction because it could potentially exceed
	// cockroachdb's transaction size limit.
	err = d.build(ir)
	if err != nil {
		// Remove the version record. This will
		// force a rebuild on the next start up.
		err1 := d.recordsdb.Delete(&Version{
			ID: foneroplugin.ID,
		}).Error
		if err1 != nil {
			panic("the cache is out of sync and will not rebuild" +
				"automatically; a rebuild must be forced")
		}
	}

	return err
}

// Setup creates the fonero plugin tables if they do not already exist.  A
// fonero plugin version record is inserted into the database during table
// creation.
func (d *fonero) Setup() error {
	log.Tracef("fonero: Setup")

	tx := d.recordsdb.Begin()
	err := d.createTables(tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}

// CheckVersion retrieves the fonero plugin version record from the database,
// if one exists, and checks that it matches the version of the current fonero
// plugin cache implementation.
func (d *fonero) CheckVersion() error {
	log.Tracef("fonero: CheckVersion")

	// Sanity check. Ensure version table exists.
	if !d.recordsdb.HasTable(tableVersions) {
		return fmt.Errorf("versions table not found")
	}

	// Lookup version record. If the version is not found or
	// if there is a version mismatch, return an error so
	// that the fonero plugin cache can be built/rebuilt.
	var v Version
	err := d.recordsdb.
		Where("id = ?", foneroplugin.ID).
		Find(&v).
		Error
	if err == gorm.ErrRecordNotFound {
		log.Debugf("version record not found for ID '%v'",
			foneroplugin.ID)
		err = cache.ErrNoVersionRecord
	} else if v.Version != foneroVersion {
		log.Debugf("version mismatch for ID '%v': got %v, want %v",
			foneroplugin.ID, v.Version, foneroVersion)
		err = cache.ErrWrongVersion
	}

	return err
}

// newFoneroPlugin returns a cache fonero plugin context.
func newFoneroPlugin(db *gorm.DB, p cache.Plugin) *fonero {
	log.Tracef("newFoneroPlugin")
	return &fonero{
		recordsdb: db,
		version:   foneroVersion,
		settings:  p.Settings,
	}
}
