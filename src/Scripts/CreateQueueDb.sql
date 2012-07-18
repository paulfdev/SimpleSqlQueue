CREATE TABLE MessageQueue (
  [MessageId] [uniqueidentifier] NOT NULL,
  [FailedAttempts] [int] NOT NULL default(0),
  [Payload] varbinary(max),
  [TimeStamp] [datetime] NOT NULL);
GO

CREATE NONCLUSTERED INDEX [IX_MessageQueue] ON [dbo].[MessageQueue] 
(
	[MessageId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
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

CREATE CLUSTERED INDEX cdxFailedMessageQueue on FailedMessageStore (Id);
GO

CREATE TABLE MessageLog (
  [FailedMessageId] [bigint] NOT NULL IDENTITY(1,1),
  FailedReason nvarchar(max));
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