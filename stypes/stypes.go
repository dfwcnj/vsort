package stypes

type Line []byte
type Lines []Line

type Kvalline struct {
	Key  Line
	Line Line
}
