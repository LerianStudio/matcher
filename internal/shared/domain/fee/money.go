package fee

import (
	"strings"

	"github.com/shopspring/decimal"
)

// validCurrencies contains only active ISO 4217 currency codes.
// Obsolete codes are excluded; historical transactions referencing them
// should be mapped to successor currencies at the ingestion boundary.
// Removed: HRK (Croatian Kuna) — Croatia adopted EUR on 2023-01-01.
var validCurrencies = map[string]struct{}{
	"AED": {}, "AFN": {}, "ALL": {}, "AMD": {}, "ANG": {},
	"AOA": {}, "ARS": {}, "AUD": {}, "AWG": {}, "AZN": {},
	"BAM": {}, "BBD": {}, "BDT": {}, "BGN": {}, "BHD": {},
	"BIF": {}, "BMD": {}, "BND": {}, "BOB": {}, "BRL": {},
	"BSD": {}, "BTN": {}, "BWP": {}, "BYN": {}, "BZD": {},
	"CAD": {}, "CDF": {}, "CHF": {}, "CLP": {}, "CNY": {},
	"COP": {}, "CRC": {}, "CUP": {}, "CVE": {}, "CZK": {},
	"DJF": {}, "DKK": {}, "DOP": {}, "DZD": {}, "EGP": {},
	"ERN": {}, "ETB": {}, "EUR": {}, "FJD": {}, "FKP": {},
	"GBP": {}, "GEL": {}, "GHS": {}, "GIP": {}, "GMD": {},
	"GNF": {}, "GTQ": {}, "GYD": {}, "HKD": {}, "HNL": {},
	"HTG": {}, "HUF": {}, "IDR": {}, "ILS": {},
	"INR": {}, "IQD": {}, "IRR": {}, "ISK": {}, "JMD": {},
	"JOD": {}, "JPY": {}, "KES": {}, "KGS": {}, "KHR": {},
	"KMF": {}, "KPW": {}, "KRW": {}, "KWD": {}, "KYD": {},
	"KZT": {}, "LAK": {}, "LBP": {}, "LKR": {}, "LRD": {},
	"LSL": {}, "LYD": {}, "MAD": {}, "MDL": {}, "MGA": {},
	"MKD": {}, "MMK": {}, "MNT": {}, "MOP": {}, "MRU": {},
	"MUR": {}, "MVR": {}, "MWK": {}, "MXN": {}, "MYR": {},
	"MZN": {}, "NAD": {}, "NGN": {}, "NIO": {}, "NOK": {},
	"NPR": {}, "NZD": {}, "OMR": {}, "PAB": {}, "PEN": {},
	"PGK": {}, "PHP": {}, "PKR": {}, "PLN": {}, "PYG": {},
	"QAR": {}, "RON": {}, "RSD": {}, "RUB": {}, "RWF": {},
	"SAR": {}, "SBD": {}, "SCR": {}, "SDG": {}, "SEK": {},
	"SGD": {}, "SHP": {}, "SLE": {}, "SOS": {}, "SRD": {},
	"SSP": {}, "STN": {}, "SVC": {}, "SYP": {}, "SZL": {},
	"THB": {}, "TJS": {}, "TMT": {}, "TND": {}, "TOP": {},
	"TRY": {}, "TTD": {}, "TWD": {}, "TZS": {}, "UAH": {},
	"UGX": {}, "USD": {}, "UYU": {}, "UZS": {}, "VES": {},
	"VND": {}, "VUV": {}, "WST": {}, "XAF": {}, "XCD": {},
	"XOF": {}, "XPF": {}, "YER": {}, "ZAR": {}, "ZMW": {},
	"ZWL": {},
}

// Money represents a monetary amount with its currency.
type Money struct {
	Amount   decimal.Decimal
	Currency string
}

// NormalizeCurrency trims whitespace, uppercases the currency code, and validates it
// against the set of supported ISO 4217 currency codes.
func NormalizeCurrency(currency string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(currency))
	if normalized == "" {
		return "", ErrInvalidCurrency
	}

	if _, ok := validCurrencies[normalized]; !ok {
		return "", ErrInvalidCurrency
	}

	return normalized, nil
}

// NewMoney creates a Money instance with a normalized currency code.
func NewMoney(amount decimal.Decimal, currency string) (Money, error) {
	normalized, err := NormalizeCurrency(currency)
	if err != nil {
		return Money{}, err
	}

	return Money{Amount: amount, Currency: normalized}, nil
}
