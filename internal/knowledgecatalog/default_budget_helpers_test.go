package knowledgecatalog

import "context"

func preflightPublicationWinnerCohort(
	cohort publicationWinnerCohort,
	candidate publicationCandidateAuthority,
) error {
	return preflightPublicationWinnerCohortTransition(
		cohort,
		candidate,
		true,
		publicationCohortCandidatePresent,
	)
}

func publicationIndexNameAdmissionAtoms(
	ctx context.Context,
	existingNames []string,
	newName string,
	slots []*publicationTransitionCanonicalObject,
) ([]publicationIndexAtom, error) {
	var budget publicationTransitionIndexMatcherBudget
	return publicationIndexNameAdmissionAtomsWithBudget(
		ctx,
		existingNames,
		newName,
		slots,
		&budget,
	)
}
