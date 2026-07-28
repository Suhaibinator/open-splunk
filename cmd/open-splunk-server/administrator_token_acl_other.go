//go:build !darwin

package main

import "os"

func validateAdministratorTokenACL(*os.File) error {
	return nil
}
