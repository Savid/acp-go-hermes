package hermesacp

func validateNativeOwnedDirectory(root string, isolation *ProcessIsolation) error {
	if isolation == nil {
		return nil
	}

	return validateNativeOwnedDirectoryPlatform(root, isolation.UID, isolation.GID)
}
