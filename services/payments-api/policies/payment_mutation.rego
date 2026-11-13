package resilientbank.payment

default allow := false

allow {
	input.operation == "payment.create"
	input.amount_cents >= 0
	input.amount_cents <= 10000000
}

allow {
	input.operation == "payment.transfer"
	input.amount_cents >= 0
	input.amount_cents <= 10000000
}
