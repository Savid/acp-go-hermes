package hermesacp

func handoffGeneratedNativeTree(root string, isolation *ProcessIsolation) error {
	if isolation == nil {
		return nil
	}

	return handoffGeneratedNativeTreePlatform(root, isolation.UID, isolation.GID)
}

func validateNativeOwnedDirectory(root string, isolation *ProcessIsolation) error {
	if isolation == nil {
		return nil
	}

	return validateNativeOwnedDirectoryPlatform(root, isolation.UID, isolation.GID)
}
