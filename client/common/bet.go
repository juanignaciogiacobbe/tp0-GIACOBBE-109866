package common

// Bet represents a bet made by a user in the system.
// It contains personal details about the user and the number they bet on.
type Bet struct {
	AgencyId  string
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

// newBet creates a new instance of the Bet structure with the provided information.
// It returns the newly created Bet object.
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

// toBytes serializes the Bet struct into a byte slice.
// It converts each field of the Bet struct into bytes, prepending each string with its length (as a single byte).
// This ensures that the deserialization process knows the length of each field.
func (b *Bet) toBytes() []byte {
	var data []byte

	// agencyId
	data = append(data, uint8(len(b.AgencyId)))
	data = append(data, []byte(b.AgencyId)...)

	// firstName
	data = append(data, uint8(len(b.FirstName)))
	data = append(data, []byte(b.FirstName)...)

	// lastName
	data = append(data, uint8(len(b.LastName)))
	data = append(data, []byte(b.LastName)...)

	// document
	data = append(data, uint8(len(b.Document)))
	data = append(data, []byte(b.Document)...)

	// birthDate
	data = append(data, uint8(len(b.Birthdate)))
	data = append(data, []byte(b.Birthdate)...)

	// number
	data = append(data, uint8(len(b.Number)))
	data = append(data, []byte(b.Number)...)

	return data
}
