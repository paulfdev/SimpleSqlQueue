CREATE TABLE [dbo].[MessageQueue](
	[MessageId] [uniqueidentifier] NOT NULL,
	[FailedAttempts] [int] NOT NULL,
	[Payload] [varbinary](max) NOT NULL,
	[TimeStamp] [datetime] NOT NULL,
 CONSTRAINT [PK_MessageQueue] PRIMARY KEY CLUSTERED 
(
	[MessageId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

ALTER TABLE [dbo].[MessageQueue] ADD  CONSTRAINT [DF__MessageQu__Faile__7E6CC920]  DEFAULT ((0)) FOR [FailedAttempts]
GO


CREATE PROCEDURE  [dbo].[Enqueue]
  @MessageId uniqueidentifier,
  @Payload varbinary(max)
AS
  SET NOCOUNT ON;
  INSERT INTO MessageQueue (MessageId, Payload, TimeStamp) VALUES (@MessageId, @Payload, GetDate());
GO

CREATE PROCEDURE [dbo].[Dequeue]
@VisibilityTimeout int
AS
  SET NOCOUNT ON;
 UPDATE 
	MessageQueue 
	SET 
		TimeStamp = DATEADD(SECOND, @VisibilityTimeout, GETDATE()),
		FailedAttempts = FailedAttempts + 1
	
   OUTPUT 
	INSERTED.MessageId,
	INSERTED.FailedAttempts,
	INSERTED.Payload,
	INSERTED.TimeStamp
	 WHERE MessageId in (Select Top(1) MessageId  FROM MessageQueue WITH(UPDLOCK , READPAST) WHERE TimeStamp < GETDATE() Order By TimeStamp )
	 
GO

CREATE TABLE [dbo].[FailedMessageStore](
	[Id] [uniqueidentifier] NOT NULL,
	[Payload] [varbinary](max) NOT NULL,
	[FailedTimeStamp] [datetime] NOT NULL,
 CONSTRAINT [PK_FailedMessageQueue] PRIMARY KEY NONCLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE CLUSTERED INDEX cdxFailedMessageQueue on FailedMessageStore (Id);
GO

CREATE TABLE [dbo].[MessageLog](
	[FailedMessageId] [uniqueidentifier] NOT NULL,
	[FailedReason] [nvarchar](max) NOT NULL,
 CONSTRAINT [PK_MessageLog] PRIMARY KEY CLUSTERED 
(
	[FailedMessageId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE PROCEDURE DeleteQueueItem
	@Id uniqueidentifier
AS
BEGIN
	SET NOCOUNT ON;
	DELETE FROM MessageQueue
	WHERE MessageId = @Id
END
GO

CREATE PROCEDURE InsertNewFailedMessage
	@Id uniqueidentifier,
	@Payload varbinary(max)
AS
	Insert into FailedMessageStore
	 (Id, Payload, FailedTimeStamp)
	 VALUES
	 (@Id, @Payload, GetDate())
	 
GO

CREATE PROCEDURE DeleteFailedQueueItem
	@Id uniqueidentifier
AS
BEGIN
	SET NOCOUNT ON;
	DELETE FROM FailedMessageStore
	WHERE Id = @Id
END
GO