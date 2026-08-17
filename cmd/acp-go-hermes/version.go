package main

var buildVersion = "dev"

func version() string {
	if buildVersion == "" {
		return "dev"
	}

	return buildVersion
}
