namespace SimpleSqlQueue
{
	public interface IQueue<T>
	{
		void Enqueue(QueueItem<T> message);
		QueueItem<T> Dequeue(int visibilityTimeout);
		void DeleteQueueItem(QueueItem<T> item);
		void StoreFailedMessage(QueueItem<T> item);
		void DeleteFailedQueueItem(QueueItem<T> item);
	}
}
