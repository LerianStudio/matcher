package services

import (
	"errors"

	"github.com/google/uuid"
)

// Sentinel errors for engine operations.
var (
	ErrNilRuleConfig        = errors.New("rule config is nil")
	ErrCandidateSetTooLarge = errors.New("candidate set too large")
	ErrEngineIsNil          = errors.New("engine is nil")
)

// Engine configuration constants.
const (
	DefaultMaxCandidates = 5000
	minMatchScore        = 60
)

// Engine executes matching rules against candidate transaction sets.
type Engine struct {
	maxCandidates int
}

// NewEngine creates a new Engine with default candidate limits.
func NewEngine() *Engine {
	return &Engine{maxCandidates: DefaultMaxCandidates}
}

// NewEngineWithLimit creates a new Engine with a custom candidate limit.
func NewEngineWithLimit(limit int) *Engine {
	return &Engine{maxCandidates: limit}
}

// maxAllowed returns the maximum candidate limit.
// Assumes engine is non-nil; all public methods check for nil receiver first.
func (engine *Engine) maxAllowed() int {
	if engine.maxCandidates <= 0 {
		return DefaultMaxCandidates
	}

	return engine.maxCandidates
}

// Execute1v1 applies rules in deterministic order and returns deterministic match proposals.
// T-E4-T2 only: 1:1 pairing.
func (engine *Engine) Execute1v1(
	rules []RuleDefinition,
	left, right []CandidateTransaction,
) ([]MatchProposal, error) {
	if engine == nil {
		return nil, ErrEngineIsNil
	}

	maxAllowed := engine.maxAllowed()
	if len(rules) > maxAllowed || len(left) > maxAllowed || len(right) > maxAllowed {
		return nil, ErrCandidateSetTooLarge
	}

	SortRules(rules)
	SortTransactions(left)
	SortTransactions(right)

	usedLeft := make(map[uuid.UUID]struct{}, len(left))
	usedRight := make(map[uuid.UUID]struct{}, len(right))

	out := make([]MatchProposal, 0, min(len(left), len(right)))

	for _, rule := range rules {
		proposals, err := findAll1v1Matches(rule, left, right, usedLeft, usedRight)
		if err != nil {
			return nil, err
		}

		for _, proposal := range proposals {
			out = append(out, proposal)

			for _, id := range proposal.LeftIDs {
				usedLeft[id] = struct{}{}
			}

			for _, id := range proposal.RightIDs {
				usedRight[id] = struct{}{}
			}
		}
	}

	return out, nil
}

// findAll1v1Matches finds ALL 1:1 matches for a given rule.
// Each left transaction can match at most one right transaction (and vice versa).
// Returns all matching pairs found for this rule.
func findAll1v1Matches(
	rule RuleDefinition,
	left, right []CandidateTransaction,
	usedLeft, usedRight map[uuid.UUID]struct{},
) ([]MatchProposal, error) {
	proposals := make([]MatchProposal, 0, min(len(left), len(right)))
	localUsedLeft := make(map[uuid.UUID]struct{}, len(left))
	localUsedRight := make(map[uuid.UUID]struct{}, len(right))

	for _, leftTxn := range left {
		if _, ok := usedLeft[leftTxn.ID]; ok {
			continue
		}

		if _, ok := localUsedLeft[leftTxn.ID]; ok {
			continue
		}

		for _, rightTxn := range right {
			if _, ok := usedRight[rightTxn.ID]; ok {
				continue
			}

			if _, ok := localUsedRight[rightTxn.ID]; ok {
				continue
			}

			matched, score, err := evalByType(rule, leftTxn, rightTxn)
			if err != nil {
				return nil, err
			}

			if !matched || score < minMatchScore {
				continue
			}

			proposals = append(proposals, MatchProposal{
				RuleID:   rule.ID,
				LeftIDs:  []uuid.UUID{leftTxn.ID},
				RightIDs: []uuid.UUID{rightTxn.ID},
				Score:    score,
				Mode:     "1:1",
			})

			localUsedLeft[leftTxn.ID] = struct{}{}
			localUsedRight[rightTxn.ID] = struct{}{}

			break
		}
	}

	return proposals, nil
}

// Execute1vN executes 1:N matching where one left transaction matches many right transactions.
func (engine *Engine) Execute1vN(
	rules []RuleDefinition,
	left, right []CandidateTransaction,
) ([]MatchProposal, error) {
	result, err := engine.Execute1vNDetailed(rules, left, right)
	if err != nil {
		return nil, err
	}

	return result.Proposals, nil
}

// Execute1vNDetailed executes 1:N matching and returns structured failure information.
func (engine *Engine) Execute1vNDetailed(
	rules []RuleDefinition,
	left, right []CandidateTransaction,
) (*EngineResult, error) {
	if engine == nil {
		return nil, ErrEngineIsNil
	}

	maxAllowed := engine.maxAllowed()
	if len(rules) > maxAllowed || len(left) > maxAllowed || len(right) > maxAllowed {
		return nil, ErrCandidateSetTooLarge
	}

	SortRules(rules)
	SortTransactions(left)
	SortTransactions(right)

	return matchWithDirectionDetailed(rules, left, right, "1:N", true)
}

// ExecuteNv1 executes N:1 matching where many left transactions match one right transaction.
func (engine *Engine) ExecuteNv1(
	rules []RuleDefinition,
	left, right []CandidateTransaction,
) ([]MatchProposal, error) {
	result, err := engine.ExecuteNv1Detailed(rules, left, right)
	if err != nil {
		return nil, err
	}

	return result.Proposals, nil
}

// ExecuteNv1Detailed executes N:1 matching and returns structured failure information.
func (engine *Engine) ExecuteNv1Detailed(
	rules []RuleDefinition,
	left, right []CandidateTransaction,
) (*EngineResult, error) {
	if engine == nil {
		return nil, ErrEngineIsNil
	}

	maxAllowed := engine.maxAllowed()
	if len(rules) > maxAllowed || len(left) > maxAllowed || len(right) > maxAllowed {
		return nil, ErrCandidateSetTooLarge
	}

	SortRules(rules)
	SortTransactions(left)
	SortTransactions(right)

	return matchWithDirectionDetailed(rules, right, left, "N:1", false)
}

type matchContext struct {
	usedPrimary   map[uuid.UUID]struct{}
	usedSecondary map[uuid.UUID]struct{}
	proposals     []MatchProposal
	allocFailures map[uuid.UUID]*AllocationFailure
	mode          string
	primaryIsLeft bool
}

// EngineResult contains the result of engine execution including any allocation failures.
type EngineResult struct {
	Proposals     []MatchProposal
	AllocFailures map[uuid.UUID]*AllocationFailure
}

func matchWithDirectionDetailed(
	rules []RuleDefinition,
	primary []CandidateTransaction,
	secondary []CandidateTransaction,
	mode string,
	primaryIsLeft bool,
) (*EngineResult, error) {
	mctx := &matchContext{
		usedPrimary:   make(map[uuid.UUID]struct{}, len(primary)),
		usedSecondary: make(map[uuid.UUID]struct{}, len(secondary)),
		proposals:     make([]MatchProposal, 0, min(len(primary), len(secondary))),
		allocFailures: make(map[uuid.UUID]*AllocationFailure),
		mode:          mode,
		primaryIsLeft: primaryIsLeft,
	}

	for _, rule := range rules {
		if err := processRuleMatches(rule, primary, secondary, mctx); err != nil {
			return nil, err
		}
	}

	return &EngineResult{
		Proposals:     mctx.proposals,
		AllocFailures: mctx.allocFailures,
	}, nil
}

func processRuleMatches(
	rule RuleDefinition,
	primary, secondary []CandidateTransaction,
	mctx *matchContext,
) error {
	for _, primaryCandidate := range primary {
		if _, ok := mctx.usedPrimary[primaryCandidate.ID]; ok {
			continue
		}

		if err := tryMatchCandidate(rule, primaryCandidate, secondary, mctx); err != nil {
			return err
		}
	}

	return nil
}

func tryMatchCandidate(
	rule RuleDefinition,
	primaryCandidate CandidateTransaction,
	secondary []CandidateTransaction,
	mctx *matchContext,
) error {
	matchedCandidates, matchedIDs, maxScore, err := findMatches(
		rule,
		primaryCandidate,
		secondary,
		mctx,
	)
	if err != nil {
		return err
	}

	if len(matchedIDs) == 0 {
		return nil
	}

	usedLeft, usedRight := resolveUsedMaps(mctx)

	applied, failure, err := applyAllocationProposalDetailed(
		rule,
		primaryCandidate,
		matchedCandidates,
		maxScore,
		&mctx.proposals,
		usedLeft,
		usedRight,
	)
	if err != nil {
		return err
	}

	if failure != nil {
		mctx.allocFailures[failure.TargetID] = failure
	}

	if !applied {
		if rule.Allocation == nil {
			addDirectProposal(rule, primaryCandidate.ID, matchedIDs, maxScore, mctx)
		}
	}

	return nil
}

func findMatches(
	rule RuleDefinition,
	primaryCandidate CandidateTransaction,
	secondary []CandidateTransaction,
	mctx *matchContext,
) ([]CandidateTransaction, []uuid.UUID, int, error) {
	if mctx.primaryIsLeft {
		return findMatchesForLeft(rule, primaryCandidate, secondary, mctx.usedSecondary)
	}

	return findMatchesForRight(rule, primaryCandidate, secondary, mctx.usedSecondary)
}

func resolveUsedMaps(mctx *matchContext) (usedLeft, usedRight map[uuid.UUID]struct{}) {
	if mctx.primaryIsLeft {
		return mctx.usedPrimary, mctx.usedSecondary
	}

	return mctx.usedSecondary, mctx.usedPrimary
}

func addDirectProposal(
	rule RuleDefinition,
	primaryID uuid.UUID,
	matchedIDs []uuid.UUID,
	maxScore int,
	mctx *matchContext,
) {
	leftIDs, rightIDs := resolveProposalIDs(primaryID, matchedIDs, mctx.primaryIsLeft)
	mctx.proposals = append(
		mctx.proposals,
		MatchProposal{
			RuleID:   rule.ID,
			LeftIDs:  leftIDs,
			RightIDs: rightIDs,
			Score:    maxScore,
			Mode:     mctx.mode,
		},
	)
	mctx.usedPrimary[primaryID] = struct{}{}
	markUsed(matchedIDs, mctx.usedSecondary)
}

func resolveProposalIDs(
	primaryID uuid.UUID,
	matchedIDs []uuid.UUID,
	primaryIsLeft bool,
) (leftIDs, rightIDs []uuid.UUID) {
	if primaryIsLeft {
		return []uuid.UUID{primaryID}, matchedIDs
	}

	return matchedIDs, []uuid.UUID{primaryID}
}

func markUsed(ids []uuid.UUID, used map[uuid.UUID]struct{}) {
	for _, id := range ids {
		used[id] = struct{}{}
	}
}

func collectMatches(
	rule RuleDefinition,
	primary CandidateTransaction,
	candidates []CandidateTransaction,
	used map[uuid.UUID]struct{},
	evalPrimaryFirst bool,
) ([]CandidateTransaction, []uuid.UUID, int, error) {
	matchedIDs := make([]uuid.UUID, 0, len(candidates))
	matchedCandidates := make([]CandidateTransaction, 0, len(candidates))
	maxScore := 0

	for _, candidate := range candidates {
		if _, ok := used[candidate.ID]; ok {
			continue
		}

		var (
			matched bool
			score   int
			err     error
		)
		if evalPrimaryFirst {
			matched, score, err = evalByType(rule, primary, candidate)
		} else {
			matched, score, err = evalByType(rule, candidate, primary)
		}

		if err != nil {
			return nil, nil, 0, err
		}

		if !matched || score < minMatchScore {
			continue
		}

		matchedIDs = append(matchedIDs, candidate.ID)
		matchedCandidates = append(matchedCandidates, candidate)

		// maxScore tracks the BEST single candidate match, not the average quality.
		// This is intentional: the group confidence represents the strongest evidence
		// for the match. For multi-candidate groups (1:N / N:1), the highest individual
		// score indicates the quality of the overall match. If individual match scores
		// per candidate are needed, review the per-item confidence instead.
		if score > maxScore {
			maxScore = score
		}
	}

	return matchedCandidates, matchedIDs, maxScore, nil
}

func findMatchesForLeft(
	rule RuleDefinition,
	left CandidateTransaction,
	right []CandidateTransaction,
	usedRight map[uuid.UUID]struct{},
) ([]CandidateTransaction, []uuid.UUID, int, error) {
	return collectMatches(rule, left, right, usedRight, true)
}

func findMatchesForRight(
	rule RuleDefinition,
	right CandidateTransaction,
	left []CandidateTransaction,
	usedLeft map[uuid.UUID]struct{},
) ([]CandidateTransaction, []uuid.UUID, int, error) {
	return collectMatches(rule, right, left, usedLeft, false)
}

func applyAllocationProposalDetailed(
	rule RuleDefinition,
	base CandidateTransaction,
	matchedCandidates []CandidateTransaction,
	maxScore int,
	out *[]MatchProposal,
	usedLeft, usedRight map[uuid.UUID]struct{},
) (bool, *AllocationFailure, error) {
	if rule.Allocation == nil {
		return false, nil, nil
	}

	result, err := buildAllocationProposalDetailed(rule, base, matchedCandidates, maxScore)
	if err != nil {
		return false, nil, err
	}

	if result.Failure != nil {
		return false, result.Failure, nil
	}

	if result.Proposal == nil {
		return false, nil, nil
	}

	*out = append(*out, *result.Proposal)

	for _, id := range result.Proposal.LeftIDs {
		usedLeft[id] = struct{}{}
	}

	for _, id := range result.Proposal.RightIDs {
		usedRight[id] = struct{}{}
	}

	return true, nil, nil
}

func evalByType(rule RuleDefinition, left, right CandidateTransaction) (bool, int, error) {
	switch {
	case rule.Exact != nil:
		matched, err := ExactMatch(left, right, rule.Exact)

		score := 0
		if matched {
			score = ScoreExactConfidence(rule.Exact, left, right)
		}

		return matched, score, err
	case rule.Tolerance != nil:
		matched, err := ToleranceMatch(left, right, rule.Tolerance)

		score := 0
		if matched {
			score = ScoreToleranceConfidence(rule.Tolerance, left, right)
		}

		return matched, score, err
	case rule.DateLag != nil:
		matched, err := DateLagMatch(left, right, rule.DateLag)

		score := 0
		if matched {
			score = ScoreDateLagConfidence(rule.DateLag, left, right)
		}

		return matched, score, err
	default:
		return false, 0, ErrNilRuleConfig
	}
}

// Deprecated: The following static scoring functions are no longer used.
// They have been replaced by the financial-first weighted scoring in confidence_scorer.go.
// The new approach implements PRD AC-001 with weighted components:
//   - Amount match: 40% weight
//   - Currency match: 30% weight
//   - Date tolerance: 20% weight
//   - Reference matching: 10% weight
