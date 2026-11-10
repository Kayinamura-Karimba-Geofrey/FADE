package features

import (
	"math"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
)

// HaversineKM returns the great-circle distance in kilometres.
func HaversineKM(lat1, lon1, lat2, lon2, earthRadiusKM float64) float64 {
	const degToRad = math.Pi / 180
	phi1 := lat1 * degToRad
	phi2 := lat2 * degToRad
	dPhi := (lat2 - lat1) * degToRad
	dLambda := (lon2 - lon1) * degToRad
	a := math.Sin(dPhi/2)*math.Sin(dPhi/2) +
		math.Cos(phi1)*math.Cos(phi2)*math.Sin(dLambda/2)*math.Sin(dLambda/2)
	return earthRadiusKM * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// NightHour reports whether t is in the configured Kigali night-hour band.
func NightHour(t time.Time, f config.Features) bool {
	h := t.In(f.Location()).Hour()
	return h >= f.Time.NightStartHour && h <= f.Time.NightEndHour
}

// NearLimitRatio is amount / daily limit for the KYC tier.
func NearLimitRatio(amount int64, tier int, f config.Features) float64 {
	limit, ok := f.KYCLimits[tier]
	if !ok || limit <= 0 {
		return 0
	}
	return float64(amount) / float64(limit)
}

// AmountStats returns the 30-day ratio and z-score for amount. count < 5,
// a non-positive mean or a zero stddev produce the documented defaults.
func AmountStats(amount, count, sum, sumsq int64, minCount int) (float64, float64) {
	if count < int64(minCount) || count == 0 || sum == 0 {
		return 1.0, 0.0
	}
	mean := float64(sum) / float64(count)
	if mean <= 0 {
		return 1.0, 0.0
	}
	variance := float64(sumsq)/float64(count) - mean*mean
	if variance <= 0 {
		return float64(amount) / mean, 0.0
	}
	std := math.Sqrt(variance)
	return float64(amount) / mean, (float64(amount) - mean) / std
}

// Ratio1h is the smoothed inbound/outbound ratio.
func Ratio1h(in, out int64) float64 {
	return float64(in+1) / float64(out+1)
}

// CicoRatio is the smoothed agent cash-in/cash-out ratio.
func CicoRatio(in, out int64) float64 {
	return float64(in+1) / float64(out+1)
}

// NightHourIn is NightHour with a pre-loaded location, avoiding a timezone
// load on every event in the hot path.
func NightHourIn(t time.Time, loc *time.Location, f config.Features) bool {
	h := t.In(loc).Hour()
	return h >= f.Time.NightStartHour && h <= f.Time.NightEndHour
}
