package value_objects

import (
	"errors"
	"strings"
)

// ErrInvalidCurrencyCode is returned when a currency code is not a valid ISO 4217 code.
var ErrInvalidCurrencyCode = errors.New("invalid currency code")

// CurrencyCode represents an ISO 4217 currency code.
type CurrencyCode string

// allowedCurrencies contains only active ISO 4217 currency codes.
// Obsolete codes are excluded; historical transactions referencing them
// should be mapped to successor currencies at the ingestion boundary.
// Removed: HRK (Croatian Kuna) — Croatia adopted EUR on 2023-01-01.
var allowedCurrencies = map[CurrencyCode]struct{}{
	"AED": {}, "AFN": {}, "ALL": {}, "AMD": {}, "ANG": {}, "AOA": {}, "ARS": {}, "AUD": {},
	"AWG": {}, "AZN": {}, "BAM": {}, "BBD": {}, "BDT": {}, "BGN": {}, "BHD": {}, "BIF": {},
	"BMD": {}, "BND": {}, "BOB": {}, "BRL": {}, "BSD": {}, "BTN": {}, "BWP": {}, "BYN": {},
	"BZD": {}, "CAD": {}, "CDF": {}, "CHF": {}, "CLP": {}, "CNY": {}, "COP": {}, "CRC": {},
	"CUP": {}, "CVE": {}, "CZK": {}, "DJF": {}, "DKK": {}, "DOP": {}, "DZD": {}, "EGP": {},
	"ERN": {}, "ETB": {}, "EUR": {}, "FJD": {}, "FKP": {}, "GBP": {}, "GEL": {}, "GHS": {},
	"GIP": {}, "GMD": {}, "GNF": {}, "GTQ": {}, "GYD": {}, "HKD": {}, "HNL": {},
	"HTG": {}, "HUF": {}, "IDR": {}, "ILS": {}, "INR": {}, "IQD": {}, "IRR": {}, "ISK": {},
	"JMD": {}, "JOD": {}, "JPY": {}, "KES": {}, "KGS": {}, "KHR": {}, "KMF": {}, "KPW": {},
	"KRW": {}, "KWD": {}, "KYD": {}, "KZT": {}, "LAK": {}, "LBP": {}, "LKR": {}, "LRD": {},
	"LSL": {}, "LYD": {}, "MAD": {}, "MDL": {}, "MGA": {}, "MKD": {}, "MMK": {}, "MNT": {},
	"MOP": {}, "MRU": {}, "MUR": {}, "MVR": {}, "MWK": {}, "MXN": {}, "MYR": {}, "MZN": {},
	"NAD": {}, "NGN": {}, "NIO": {}, "NOK": {}, "NPR": {}, "NZD": {}, "OMR": {}, "PAB": {},
	"PEN": {}, "PGK": {}, "PHP": {}, "PKR": {}, "PLN": {}, "PYG": {}, "QAR": {}, "RON": {},
	"RSD": {}, "RUB": {}, "RWF": {}, "SAR": {}, "SBD": {}, "SCR": {}, "SDG": {}, "SEK": {},
	"SGD": {}, "SHP": {}, "SLE": {}, "SOS": {}, "SRD": {}, "SSP": {}, "STN": {}, "SVC": {},
	"SYP": {}, "SZL": {}, "THB": {}, "TJS": {}, "TMT": {}, "TND": {}, "TOP": {}, "TRY": {},
	"TTD": {}, "TWD": {}, "TZS": {}, "UAH": {}, "UGX": {}, "USD": {}, "UYU": {}, "UZS": {},
	"VES": {}, "VND": {}, "VUV": {}, "WST": {}, "XAF": {}, "XCD": {}, "XOF": {}, "XPF": {},
	"YER": {}, "ZAR": {}, "ZMW": {}, "ZWL": {},
}

// IsValid returns true if the currency code is a valid ISO 4217 code.
func (c CurrencyCode) IsValid() bool {
	_, ok := allowedCurrencies[c]
	return ok
}

func (c CurrencyCode) String() string {
	return string(c)
}

// ParseCurrencyCode parses and validates a currency code string.
func ParseCurrencyCode(value string) (CurrencyCode, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", ErrInvalidCurrencyCode
	}

	code := CurrencyCode(strings.ToUpper(trimmed))
	if !code.IsValid() {
		return "", ErrInvalidCurrencyCode
	}

	return code, nil
}
