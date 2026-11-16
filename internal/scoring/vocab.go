package scoring

// EncodeTxnType maps a transaction type to its frozen vocabulary code.
// Unknown values map to 0, matching config/ml_vocab.yaml.
func EncodeTxnType(v string) int {
	switch v {
	case "AIRTIME":
		return 1
	case "BANK_TRANSFER":
		return 2
	case "BILL_PAY":
		return 3
	case "CASH_IN":
		return 4
	case "CASH_OUT":
		return 5
	case "MERCHANT_PAY":
		return 6
	case "P2P_SEND":
		return 7
	default:
		return 0
	}
}

// EncodeChannel maps a channel to its frozen vocabulary code.
func EncodeChannel(v string) int {
	switch v {
	case "AGENT":
		return 1
	case "API":
		return 2
	case "APP":
		return 3
	case "USSD":
		return 4
	default:
		return 0
	}
}
