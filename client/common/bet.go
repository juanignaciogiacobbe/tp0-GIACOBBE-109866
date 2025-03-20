package common

type Bet struct {
	AgencyId  string
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

func newBet(agencyId string, firstName string, lastName string, document string, birthDate string, number string) Bet {
	return Bet{
		AgencyId:  agencyId,
		FirstName: firstName,
		LastName:  lastName,
		Document:  document,
		Birthdate: birthDate,
		Number:    number,
	}
}

func (b *Bet) toBytes() []byte {
	var data []byte

	data = append(data, uint8(len(b.AgencyId)))
	data = append(data, []byte(b.AgencyId)...)

	data = append(data, uint8(len(b.FirstName)))
	data = append(data, []byte(b.FirstName)...)

	data = append(data, uint8(len(b.LastName)))
	data = append(data, []byte(b.LastName)...)

	data = append(data, uint8(len(b.Document)))
	data = append(data, []byte(b.Document)...)

	data = append(data, uint8(len(b.Birthdate)))
	data = append(data, []byte(b.Birthdate)...)

	data = append(data, uint8(len(b.Number)))
	data = append(data, []byte(b.Number)...)

	return data
}
