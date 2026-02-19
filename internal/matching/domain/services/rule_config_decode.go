package services

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ErrRuleConfigDecode is returned when rule configuration decoding fails.
var ErrRuleConfigDecode = errors.New("rule config decode error")

// Static errors for parseIntValue to satisfy err113 linter.
var (
	errOutOfRange = errors.New("out of range")
	errMustBeInt  = errors.New("must be integer")
)

// ValidateRuleConfig validates that a rule's config matches the expected schema for its type.
// This function can be used during rule creation/update to ensure invalid rules are rejected early.
func ValidateRuleConfig(ruleType shared.RuleType, config map[string]any) error {
	if config == nil {
		return wrapDecodeErr("config is nil")
	}

	if len(config) == 0 {
		return wrapDecodeErr("config is empty")
	}

	rule := &shared.MatchRule{
		Type:   ruleType,
		Config: config,
	}

	_, err := DecodeRuleDefinition(rule)
	if err != nil {
		return err
	}

	return nil
}

// Default values for rule configuration.
const (
	maxRoundingScale      = 10
	maxDateWindowDays     = 3650
	maxDateLagDays        = 3650
	defaultExactScore     = 100
	defaultExactBaseScore = 90
	defaultToleranceScore = 85
	defaultToleranceBase  = 80
	defaultDateLagScore   = 80
	defaultRoundingScale  = 2
)

// DecodeRuleDefinition decodes a shared MatchRule into a RuleDefinition.
func DecodeRuleDefinition(rule *shared.MatchRule) (RuleDefinition, error) {
	def, err := RuleDefinitionFromMatchRule(rule)
	if err != nil {
		return RuleDefinition{}, err
	}

	switch def.Type {
	case shared.RuleTypeExact:
		return decodeExactRule(rule.Config, def)
	case shared.RuleTypeTolerance:
		return decodeToleranceRule(rule.Config, def)
	case shared.RuleTypeDateLag:
		return decodeDateLagRule(rule.Config, def)
	default:
		return RuleDefinition{}, ErrUnsupportedRuleType
	}
}

func decodeExactRule(config map[string]any, def RuleDefinition) (RuleDefinition, error) {
	cfg, err := decodeExactConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	def.Exact = &cfg

	def.Allocation, err = decodeAllocationConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	alignBaseAmountSettings(config, def.Allocation, &cfg.MatchBaseAmount, &cfg.MatchBaseCurrency)

	return def, nil
}

func decodeToleranceRule(config map[string]any, def RuleDefinition) (RuleDefinition, error) {
	cfg, err := decodeToleranceConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	def.Tolerance = &cfg

	def.Allocation, err = decodeAllocationConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	alignBaseAmountSettings(config, def.Allocation, &cfg.MatchBaseAmount, &cfg.MatchBaseCurrency)

	return def, nil
}

func decodeDateLagRule(config map[string]any, def RuleDefinition) (RuleDefinition, error) {
	cfg, err := decodeDateLagConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	def.DateLag = &cfg

	def.Allocation, err = decodeAllocationConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	return def, nil
}

func alignBaseAmountSettings(
	config map[string]any,
	allocation *AllocationConfig,
	matchBaseAmount *bool,
	matchBaseCurrency *bool,
) {
	// Bidirectional alignment: allocation.UseBaseAmount ↔ matching.MatchBaseAmount
	// If allocation explicitly uses base amounts, enable base matching
	if allocation != nil && allocation.UseBaseAmount {
		if matchBaseAmount != nil {
			*matchBaseAmount = true
		}

		if matchBaseCurrency != nil {
			*matchBaseCurrency = true
		}
	}

	// If base matching is enabled and allocationUseBaseAmount wasn't explicitly set to false,
	// align allocation to also use base amounts for consistency
	if allocation != nil && matchBaseAmount != nil && *matchBaseAmount &&
		!hasExplicitFalse(config, "allocationUseBaseAmount") {
		allocation.UseBaseAmount = true
	}
}

func decodeExactConfig(configMap map[string]any) (ExactConfig, error) {
	cfg := defaultExactConfig()

	if err := decodeExactBoolFields(configMap, &cfg); err != nil {
		return ExactConfig{}, err
	}

	if err := decodeExactScoreFields(configMap, &cfg); err != nil {
		return ExactConfig{}, err
	}

	if err := decodeExactDatePrecision(configMap, &cfg); err != nil {
		return ExactConfig{}, err
	}

	return cfg, nil
}

func defaultExactConfig() ExactConfig {
	return ExactConfig{
		MatchAmount:       true,
		MatchCurrency:     true,
		MatchDate:         true,
		DatePrecision:     DatePrecisionDay,
		MatchReference:    true,
		CaseInsensitive:   true,
		MatchBaseAmount:   false,
		MatchBaseCurrency: false,
		MatchScore:        defaultExactScore,
		MatchBaseScore:    defaultExactBaseScore,
	}
}

func decodeExactBoolFields(configMap map[string]any, cfg *ExactConfig) error {
	var err error

	cfg.MatchAmount, err = getBool(configMap, "matchAmount", cfg.MatchAmount)
	if err != nil {
		return err
	}

	cfg.MatchCurrency, err = getBool(configMap, "matchCurrency", cfg.MatchCurrency)
	if err != nil {
		return err
	}

	cfg.MatchDate, err = getBool(configMap, "matchDate", cfg.MatchDate)
	if err != nil {
		return err
	}

	cfg.MatchReference, err = getBool(configMap, "matchReference", cfg.MatchReference)
	if err != nil {
		return err
	}

	cfg.CaseInsensitive, err = getBool(configMap, "caseInsensitive", cfg.CaseInsensitive)
	if err != nil {
		return err
	}

	cfg.ReferenceMustSet, err = getBool(configMap, "referenceMustSet", false)
	if err != nil {
		return err
	}

	cfg.MatchBaseAmount, err = getBool(configMap, "matchBaseAmount", cfg.MatchBaseAmount)
	if err != nil {
		return err
	}

	cfg.MatchBaseCurrency, err = getBool(configMap, "matchBaseCurrency", cfg.MatchBaseCurrency)
	if err != nil {
		return err
	}

	allocationUseBaseAmount, err := getBool(configMap, "allocationUseBaseAmount", false)
	if err != nil {
		return err
	}

	if allocationUseBaseAmount {
		cfg.MatchBaseAmount = true
		cfg.MatchBaseCurrency = true
	}

	return nil
}

func decodeExactScoreFields(configMap map[string]any, cfg *ExactConfig) error {
	var err error

	cfg.MatchScore, err = getInt(configMap, "matchScore", cfg.MatchScore)
	if err != nil {
		return err
	}

	cfg.MatchBaseScore, err = getInt(configMap, "matchBaseScore", cfg.MatchBaseScore)
	if err != nil {
		return err
	}

	if err := validateScore(cfg.MatchScore); err != nil {
		return err
	}

	if cfg.MatchBaseAmount || cfg.MatchBaseCurrency {
		if err := validateScore(cfg.MatchBaseScore); err != nil {
			return err
		}
	}

	return nil
}

func decodeExactDatePrecision(configMap map[string]any, cfg *ExactConfig) error {
	value, ok := configMap["datePrecision"]
	if !ok {
		return nil
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("datePrecision must be string")
	}

	switch DatePrecision(strVal) {
	case DatePrecisionDay, DatePrecisionTimestamp:
		cfg.DatePrecision = DatePrecision(strVal)
	default:
		return wrapDecodeErr("invalid datePrecision")
	}

	return nil
}

func decodeToleranceConfig(configMap map[string]any) (ToleranceConfig, error) {
	cfg, err := initToleranceConfig(configMap)
	if err != nil {
		return ToleranceConfig{}, err
	}

	if err := validateToleranceBounds(cfg); err != nil {
		return ToleranceConfig{}, err
	}

	if err := decodeToleranceRoundingMode(configMap, &cfg); err != nil {
		return ToleranceConfig{}, err
	}

	if err := decodeTolerancePercentageBase(configMap, &cfg); err != nil {
		return ToleranceConfig{}, err
	}

	if err := decodeToleranceAmounts(configMap, &cfg); err != nil {
		return ToleranceConfig{}, err
	}

	if err := decodeToleranceMatchFields(configMap, &cfg); err != nil {
		return ToleranceConfig{}, err
	}

	return cfg, nil
}

func initToleranceConfig(configMap map[string]any) (ToleranceConfig, error) {
	absDefault, err := decimal.NewFromString("0.50")
	if err != nil {
		return ToleranceConfig{}, wrapDecodeErr("invalid default absTolerance")
	}

	pctDefault, err := decimal.NewFromString("0.005")
	if err != nil {
		return ToleranceConfig{}, wrapDecodeErr("invalid default percentTolerance")
	}

	dateWindowDays, err := getInt(configMap, "dateWindowDays", 0)
	if err != nil {
		return ToleranceConfig{}, err
	}

	roundingScale, err := getInt(configMap, "roundingScale", defaultRoundingScale)
	if err != nil {
		return ToleranceConfig{}, err
	}

	return ToleranceConfig{
		MatchCurrency:      true,
		DateWindowDays:     dateWindowDays,
		RoundingScale:      roundingScale,
		RoundingMode:       RoundingHalfUp,
		AbsAmountTolerance: absDefault,
		PercentTolerance:   pctDefault,
		MatchBaseAmount:    false,
		MatchBaseCurrency:  false,
		MatchScore:         defaultToleranceScore,
		MatchBaseScore:     defaultToleranceBase,
		PercentageBase:     TolerancePercentageBaseMax,
	}, nil
}

func validateToleranceBounds(cfg ToleranceConfig) error {
	if cfg.DateWindowDays < 0 {
		return wrapDecodeErr("dateWindowDays must be >= 0")
	}

	if cfg.DateWindowDays > maxDateWindowDays {
		return wrapDecodeErr("dateWindowDays exceeds maximum")
	}

	if cfg.RoundingScale < 0 {
		return wrapDecodeErr("roundingScale must be >= 0")
	}

	if cfg.RoundingScale > maxRoundingScale {
		return wrapDecodeErr("roundingScale exceeds maximum")
	}

	return nil
}

func decodeToleranceRoundingMode(configMap map[string]any, cfg *ToleranceConfig) error {
	value, ok := configMap["roundingMode"]
	if !ok {
		return nil
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("roundingMode must be string")
	}

	switch RoundingMode(strVal) {
	case RoundingHalfUp, RoundingBankers, RoundingFloor, RoundingCeil, RoundingTruncate:
		cfg.RoundingMode = RoundingMode(strVal)
	default:
		return wrapDecodeErr("invalid roundingMode")
	}

	return nil
}

func decodeTolerancePercentageBase(configMap map[string]any, cfg *ToleranceConfig) error {
	value, ok := configMap["percentageBase"]
	if !ok {
		return nil // Use default (MAX)
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("percentageBase must be string")
	}

	normalized := TolerancePercentageBase(strings.ToUpper(strVal))

	switch normalized {
	case TolerancePercentageBaseMax, TolerancePercentageBaseMin,
		TolerancePercentageBaseAvg, TolerancePercentageBaseLeft,
		TolerancePercentageBaseRight:
		cfg.PercentageBase = normalized
	default:
		return wrapDecodeErr("invalid percentageBase: must be MAX, MIN, AVERAGE, LEFT, or RIGHT")
	}

	return nil
}

func decodeToleranceAmounts(configMap map[string]any, cfg *ToleranceConfig) error {
	absTol, err := getDecimal(configMap, "absTolerance", cfg.AbsAmountTolerance)
	if err != nil {
		return err
	}

	pctTol, err := getDecimal(configMap, "percentTolerance", cfg.PercentTolerance)
	if err != nil {
		return err
	}

	if absTol.IsNegative() {
		return wrapDecodeErr("absTolerance must be non-negative")
	}

	if pctTol.IsNegative() {
		return wrapDecodeErr("percentTolerance must be non-negative")
	}

	cfg.AbsAmountTolerance = absTol
	cfg.PercentTolerance = pctTol

	return nil
}

func decodeToleranceMatchFields(configMap map[string]any, cfg *ToleranceConfig) error {
	var err error

	cfg.MatchCurrency, err = getBool(configMap, "matchCurrency", cfg.MatchCurrency)
	if err != nil {
		return err
	}

	cfg.MatchBaseAmount, err = getBool(configMap, "matchBaseAmount", cfg.MatchBaseAmount)
	if err != nil {
		return err
	}

	cfg.MatchBaseCurrency, err = getBool(configMap, "matchBaseCurrency", cfg.MatchBaseCurrency)
	if err != nil {
		return err
	}

	cfg.MatchScore, err = getInt(configMap, "matchScore", cfg.MatchScore)
	if err != nil {
		return err
	}

	cfg.MatchBaseScore, err = getInt(configMap, "matchBaseScore", cfg.MatchBaseScore)
	if err != nil {
		return err
	}

	if err := validateScore(cfg.MatchScore); err != nil {
		return err
	}

	if cfg.MatchBaseAmount || cfg.MatchBaseCurrency {
		if err := validateScore(cfg.MatchBaseScore); err != nil {
			return err
		}
	}

	if err := decodeToleranceReferenceFields(configMap, cfg); err != nil {
		return err
	}

	return nil
}

// decodeToleranceReferenceFields parses reference matching fields for tolerance rules.
// Defaults match ExactConfig for consistency: matchReference=true, caseInsensitive=true.
func decodeToleranceReferenceFields(configMap map[string]any, cfg *ToleranceConfig) error {
	var err error

	cfg.MatchReference, err = getBool(configMap, "matchReference", true)
	if err != nil {
		return err
	}

	cfg.CaseInsensitive, err = getBool(configMap, "caseInsensitive", true)
	if err != nil {
		return err
	}

	cfg.ReferenceMustSet, err = getBool(configMap, "referenceMustSet", false)
	if err != nil {
		return err
	}

	return nil
}

func decodeAllocationConfig(configMap map[string]any) (*AllocationConfig, error) {
	cfg, err := initAllocationConfig(configMap)
	if err != nil {
		return nil, err
	}

	if err := decodeAllocationDirection(configMap, &cfg); err != nil {
		return nil, err
	}

	if err := decodeAllocationToleranceMode(configMap, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func initAllocationConfig(configMap map[string]any) (AllocationConfig, error) {
	allowPartial, err := getBool(configMap, "allowPartial", false)
	if err != nil {
		return AllocationConfig{}, err
	}

	useBaseAmount, err := getBool(configMap, "allocationUseBaseAmount", false)
	if err != nil {
		return AllocationConfig{}, err
	}

	toleranceValue, err := getDecimal(configMap, "allocationToleranceValue", decimal.Zero)
	if err != nil {
		return AllocationConfig{}, err
	}

	if toleranceValue.IsNegative() {
		return AllocationConfig{}, wrapDecodeErr("allocationToleranceValue must be non-negative")
	}

	return AllocationConfig{
		AllowPartial:   allowPartial,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: toleranceValue,
		UseBaseAmount:  useBaseAmount,
	}, nil
}

func decodeAllocationDirection(configMap map[string]any, cfg *AllocationConfig) error {
	value, ok := configMap["allocationDirection"]
	if !ok {
		return nil
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("allocationDirection must be string")
	}

	switch AllocationDirection(strVal) {
	case AllocationDirectionLeftToRight, AllocationDirectionRightToLeft:
		cfg.Direction = AllocationDirection(strVal)
	default:
		return wrapDecodeErr("invalid allocationDirection")
	}

	return nil
}

func decodeAllocationToleranceMode(configMap map[string]any, cfg *AllocationConfig) error {
	value, ok := configMap["allocationToleranceMode"]
	if !ok {
		return nil
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("allocationToleranceMode must be string")
	}

	switch AllocationToleranceMode(strVal) {
	case AllocationToleranceAbsolute, AllocationTolerancePercent:
		cfg.ToleranceMode = AllocationToleranceMode(strVal)
	default:
		return wrapDecodeErr("invalid allocationToleranceMode")
	}

	return nil
}

func decodeDateLagConfig(configMap map[string]any) (DateLagConfig, error) {
	cfg, err := initDateLagConfig(configMap)
	if err != nil {
		return DateLagConfig{}, err
	}

	if err := validateDateLagBounds(cfg); err != nil {
		return DateLagConfig{}, err
	}

	if err := decodeDateLagDirection(configMap, &cfg); err != nil {
		return DateLagConfig{}, err
	}

	if err := decodeDateLagMatchFields(configMap, &cfg); err != nil {
		return DateLagConfig{}, err
	}

	return cfg, nil
}

func initDateLagConfig(configMap map[string]any) (DateLagConfig, error) {
	minDays, err := getInt(configMap, "minDays", 0)
	if err != nil {
		return DateLagConfig{}, err
	}

	maxDays, err := getInt(configMap, "maxDays", 0)
	if err != nil {
		return DateLagConfig{}, err
	}

	inclusive, err := getBool(configMap, "inclusive", true)
	if err != nil {
		return DateLagConfig{}, err
	}

	feeTolerance, err := getDecimal(configMap, "feeTolerance", decimal.Zero)
	if err != nil {
		return DateLagConfig{}, err
	}

	if feeTolerance.IsNegative() {
		return DateLagConfig{}, wrapDecodeErr("feeTolerance must be non-negative")
	}

	return DateLagConfig{
		MinDays:       minDays,
		MaxDays:       maxDays,
		Inclusive:     inclusive,
		Direction:     DateLagDirectionAbs,
		FeeTolerance:  feeTolerance,
		MatchScore:    defaultDateLagScore,
		MatchCurrency: true,
	}, nil
}

func validateDateLagBounds(cfg DateLagConfig) error {
	if cfg.MinDays < 0 || cfg.MaxDays < 0 {
		return wrapDecodeErr("minDays/maxDays must be >= 0")
	}

	if cfg.MaxDays < cfg.MinDays {
		return wrapDecodeErr("maxDays must be >= minDays")
	}

	if cfg.MinDays > maxDateLagDays || cfg.MaxDays > maxDateLagDays {
		return wrapDecodeErr("date lag days exceed maximum")
	}

	// Reject Inclusive=false with MinDays=0: this combination excludes same-day
	// transactions (diff=0 fails diff > 0), which is almost certainly a misconfiguration.
	// Use Inclusive=true with MinDays=0 to include same-day transactions.
	if !cfg.Inclusive && cfg.MinDays == 0 {
		return wrapDecodeErr(
			"exclusive bounds (inclusive=false) with minDays=0 excludes same-day transactions; " +
				"use inclusive=true with minDays=0 to include same-day matches, " +
				"or set minDays=1 for exclusive bounds starting from 1 day",
		)
	}

	return nil
}

func decodeDateLagDirection(configMap map[string]any, cfg *DateLagConfig) error {
	value, ok := configMap["direction"]
	if !ok {
		return nil
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("direction must be string")
	}

	switch DateLagDirection(strVal) {
	case DateLagDirectionAbs, DateLagDirectionLeftBeforeRight, DateLagDirectionRightBeforeLeft:
		cfg.Direction = DateLagDirection(strVal)
	default:
		return wrapDecodeErr("invalid direction")
	}

	return nil
}

func decodeDateLagMatchFields(configMap map[string]any, cfg *DateLagConfig) error {
	var err error

	cfg.MatchCurrency, err = getBool(configMap, "matchCurrency", cfg.MatchCurrency)
	if err != nil {
		return err
	}

	cfg.MatchScore, err = getInt(configMap, "matchScore", cfg.MatchScore)
	if err != nil {
		return err
	}

	return validateScore(cfg.MatchScore)
}

func getBool(configMap map[string]any, key string, def bool) (bool, error) {
	value, ok := configMap[key]
	if !ok || value == nil {
		return def, nil
	}

	boolVal, ok := value.(bool)
	if !ok {
		return def, wrapDecodeErr(key + " must be bool")
	}

	return boolVal, nil
}

// hasExplicitFalse checks if a key exists in the config and is explicitly set to false.
// Returns false if the key is missing or set to any other value.
func hasExplicitFalse(configMap map[string]any, key string) bool {
	value, ok := configMap[key]
	if !ok {
		return false
	}

	boolVal, ok := value.(bool)
	if !ok {
		return false
	}

	return !boolVal
}

func getInt(configMap map[string]any, key string, def int) (int, error) {
	value, ok := configMap[key]
	if !ok || value == nil {
		return def, nil
	}

	parsed, err := parseIntValue(value)
	if err != nil {
		return def, wrapDecodeErr(key + ": " + err.Error())
	}

	return parsed, nil
}

func parseIntValue(value any) (int, error) {
	switch num := value.(type) {
	case int:
		return num, nil
	case int32:
		return int(num), nil
	case int64:
		return parseInt64Value(num)
	case float64:
		return parseFloat64Value(num)
	case json.Number:
		return parseJSONNumberValue(num)
	case string:
		return parseStringValue(num)
	default:
		return parseReflectValue(value)
	}
}

func parseInt64Value(num int64) (int, error) {
	if int64(int(num)) != num {
		return 0, errOutOfRange
	}

	return int(num), nil
}

func parseFloat64Value(num float64) (int, error) {
	maxInt := float64(^uint(0) >> 1)
	minInt := -maxInt - 1

	if math.Trunc(num) != num {
		return 0, errMustBeInt
	}

	if num > maxInt || num < minInt {
		return 0, errOutOfRange
	}

	return int(num), nil
}

func parseJSONNumberValue(num json.Number) (int, error) {
	parsed, err := num.Int64()
	if err != nil {
		return 0, errMustBeInt
	}

	if int64(int(parsed)) != parsed {
		return 0, errOutOfRange
	}

	return int(parsed), nil
}

func parseStringValue(num string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(num))
	if err != nil {
		return 0, errMustBeInt
	}

	return parsed, nil
}

func parseReflectValue(value any) (int, error) {
	v := reflect.ValueOf(value)
	if !v.IsValid() || v.Kind() < reflect.Int || v.Kind() > reflect.Int64 {
		return 0, errMustBeInt
	}

	intVal := v.Int()
	if int64(int(intVal)) != intVal {
		return 0, errOutOfRange
	}

	return int(intVal), nil
}

func getDecimal(
	configMap map[string]any,
	key string,
	def decimal.Decimal,
) (decimal.Decimal, error) {
	value, ok := configMap[key]
	if !ok || value == nil {
		return def, nil
	}

	switch typedVal := value.(type) {
	case string:
		decVal, err := decimal.NewFromString(typedVal)
		if err != nil {
			return decimal.Decimal{}, wrapDecodeErr(key + " invalid decimal string")
		}

		return decVal, nil
	case float64:
		floatStr := strconv.FormatFloat(typedVal, 'f', -1, 64)

		decVal, err := decimal.NewFromString(floatStr)
		if err != nil {
			return decimal.Decimal{}, wrapDecodeErr(key + " invalid float value")
		}

		return decVal, nil
	case int:
		return decimal.NewFromInt(int64(typedVal)), nil
	case int32:
		return decimal.NewFromInt(int64(typedVal)), nil
	case int64:
		return decimal.NewFromInt(typedVal), nil
	case json.Number:
		decVal, err := decimal.NewFromString(typedVal.String())
		if err != nil {
			return decimal.Decimal{}, wrapDecodeErr(key + " invalid json.Number")
		}

		return decVal, nil
	default:
		return decimal.Decimal{}, wrapDecodeErr(key + " unsupported type")
	}
}

func wrapDecodeErr(msg string) error {
	return fmt.Errorf("%w: %s", ErrRuleConfigDecode, msg)
}

func validateScore(score int) error {
	if score < 0 || score > 100 {
		return wrapDecodeErr("score must be between 0 and 100")
	}

	return nil
}
