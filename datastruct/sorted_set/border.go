package sorted_set

import (
	"errors"
	"strconv"
)

const (
	scoreNegativeInf int8 = -1
	scorePositiveInf int8 = 1
	lexNegativeInf   int8 = '-'
	lexPositiveInf   int8 = '+'
)

var scorePositiveInfBorder = &ScoreBorder{
	Inf: scorePositiveInf,
}

var scoreNegativeInfBorder = &ScoreBorder{
	Inf: scoreNegativeInf,
}

var lexPositiveInfBorder = &ScoreBorder{
	Inf: lexPositiveInf,
}

var lexNegativeInfBorder = &ScoreBorder{
	Inf: lexNegativeInf,
}

type Border interface {
	greater(element *Element) bool
	less(element *Element) bool
	getValue() interface{}
	getExclude() bool
	isIntersected(max Border) bool
}

type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

func (b *ScoreBorder) greater(element *Element) bool {
	if b.Inf == scoreNegativeInf {
		return false
	} else if b.Inf == scorePositiveInf {
		return true
	}
	if b.Exclude {
		return b.Value > element.Score
	}
	return b.Value >= element.Score
}

func (b *ScoreBorder) less(element *Element) bool {
	if b.Inf == scoreNegativeInf {
		return true
	} else if b.Inf == scorePositiveInf {
		return false
	}
	if b.Exclude {
		return b.Value < element.Score
	}
	return b.Value <= element.Score
}

func (b *ScoreBorder) getValue() interface{} {
	return b.Value
}

func (b *ScoreBorder) getExclude() bool {
	return b.Exclude
}

func (b *ScoreBorder) isIntersected(max Border) bool {
	minValue := b.Value
	maxValue := max.(*ScoreBorder).Value
	return minValue > maxValue || (minValue == maxValue && (b.getExclude() || max.getExclude()))
}

func ParseScoreBorder(s string) (Border, error) {
	if s == "inf" || s == "+inf" {
		return scorePositiveInfBorder, nil
	} else if s == "-inf" {
		return scoreNegativeInfBorder, nil
	}
	if s[0] == '(' {
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, errors.New("max or min is not float64")
		}
		return &ScoreBorder{
			Inf:     0,
			Value:   value,
			Exclude: true,
		}, nil
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, errors.New("max or min is not float64")
	}
	return &ScoreBorder{
		Inf:     0,
		Value:   value,
		Exclude: false,
	}, nil
}

// LexBorder represents range of a string value, including: <, <=, >, >=, +, -
type LexBorder struct {
	Inf     int8
	Value   string
	Exclude bool
}

// if max.greater(lex) then the lex is within the upper border
// do not use min.greater()
func (border *LexBorder) greater(element *Element) bool {
	value := element.Member
	if border.Inf == lexNegativeInf {
		return false
	} else if border.Inf == lexPositiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

func (border *LexBorder) less(element *Element) bool {
	value := element.Member
	if border.Inf == lexNegativeInf {
		return true
	} else if border.Inf == lexPositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

func (border *LexBorder) getValue() interface{} {
	return border.Value
}

func (border *LexBorder) getExclude() bool {
	return border.Exclude
}

// ParseLexBorder creates LexBorder from redis arguments
func ParseLexBorder(s string) (Border, error) {
	if s == "+" {
		return lexPositiveInfBorder, nil
	}
	if s == "-" {
		return lexNegativeInfBorder, nil
	}
	if s[0] == '(' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: true,
		}, nil
	}

	if s[0] == '[' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: false,
		}, nil
	}

	return nil, errors.New("ERR min or max not valid string range item")
}

func (border *LexBorder) isIntersected(max Border) bool {
	minValue := border.Value
	maxValue := max.(*LexBorder).Value
	return border.Inf == '+' || minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}
