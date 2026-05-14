package main

func logIfErr(fn func() error, what string) {
	if err := fn(); err != nil {
		log.Errorf("Failed %s: %s", what, err)
	}
}
