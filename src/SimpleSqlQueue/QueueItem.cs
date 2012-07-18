using System;

namespace SimpleSqlQueue
{
	public class QueueItem<T>
	{
		public Guid Id { get; set; }
		public T Payload { get; set; }
		public int FailedAttempts { get; set; }
		public DateTime TimeStamp { get; set; }

		public QueueItem()
		{
			Id = Guid.NewGuid();
		}

		public QueueItem(Guid id)
		{
			Id = id;
		}
	}
}
