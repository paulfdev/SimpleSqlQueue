using System;

namespace SimpleSqlQueue
{
	public class QueueItem<T>
	{
		public Guid Id { get; private set; }
		public T Payload { get; private set; }
		public int FailedAttempts { get; private set; }
		public DateTime? TimeStamp { get; private set; }

		public QueueItem(T payLoad)
		{
			Id = Guid.NewGuid();
			Payload = payLoad;
		}

		public QueueItem(Guid id, T payLoad, int failedAttempts, DateTime timeStamp)
		{
			Id = id;
			Payload = payLoad;
			FailedAttempts = failedAttempts;
			TimeStamp = timeStamp;
		}
	}
}
