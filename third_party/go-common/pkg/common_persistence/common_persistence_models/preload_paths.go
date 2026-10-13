package common_persistence_models

// Preload path constants for GORM queries.
const (
	PreloadAvatarAttachment               = "AvatarAttachment"
	PreloadApplicantUser                  = "ApplicantUser"
	PreloadCreatedByUser                  = "CreatedByUser"
	PreloadCreatedByUserProfilePicture    = "CreatedByUser.ProfilePictureAttachment"
	PreloadEditedByUser                   = "EditedByUser"
	PreloadEditedByUserProfilePicture     = "EditedByUser.ProfilePictureAttachment"
	PreloadEmbeds                         = "Embeds"
	PreloadInvitedByUser                  = "InvitedByUser"
	PreloadMentions                       = "Mentions"
	PreloadMentionsUser                   = "Mentions.User"
	PreloadOrganization                   = "Organization"
	PreloadParticipants                   = "Participants"
	PreloadParticipantsUser               = "Participants.User"
	PreloadProfilePictureAttachment       = "ProfilePictureAttachment"
	PreloadReactions                      = "Reactions"
	PreloadReactionsUser                  = "Reactions.User"
	PreloadSender                         = "Sender"
	PreloadSenderProfilePictureAttachment = "Sender.ProfilePictureAttachment"
	PreloadToolCall                       = "ToolCall"
	PreloadResumeAttachment               = "ResumeAttachment"
	PreloadUser                           = "User"
	PreloadUserProfilePictureAttachment   = "User.ProfilePictureAttachment"
)
