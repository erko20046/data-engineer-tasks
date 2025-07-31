package turkey

import "time"

type Companies struct {
	Id                  int64 `gorm:"primaryKey;autoIncrement"`
	Uin                 string
	ChamberRegNo        *string
	CentralRegSystemNo  *string
	Name                string
	StatusId            *int8
	RegDate             *time.Time
	AddressId           *int64
	MainContractRegDate *time.Time
	TaxNumberId         int64
	Capital             *float64
	OccupationGroupId   *int64
	Relevance           time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
