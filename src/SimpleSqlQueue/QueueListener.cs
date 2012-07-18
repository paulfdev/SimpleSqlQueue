using System;
using System.Threading;

namespace SimpleSqlQueue
{
	public class QueueListener<T>
	{
		bool _isRunning;
		readonly IQueue<T> _queue;
		readonly int _retriesBeforeFailure;
		readonly int _visibilityTimeout;

		public QueueListener(IQueue<T> queue, int retriesBeforeFailure, int visibilityTimeout)
		{
			_queue = queue;
			_retriesBeforeFailure = retriesBeforeFailure;
			_visibilityTimeout = visibilityTimeout;
		}

		public void StartListening(Action<QueueItem<T>> processingAction)
		{
			_isRunning = true;
			ThreadPool.QueueUserWorkItem(delegate { MonitorQueue(processingAction); });
		}

		public void StopListening()
		{
			_isRunning = false;
		}

		void LogFailure(string source, string message)
		{
		}

		void MonitorQueue(Action<QueueItem<T>> action)
		{
			while (_isRunning)
			{
				Thread.Sleep(100);
				var item = _queue.Dequeue(_visibilityTimeout);
				if (item != null)
				{
					try
					{
						action.Invoke(item);
						_queue.DeleteQueueItem(item);
					}
					catch (Exception e)
					{
						if (item.FailedAttempts >= _retriesBeforeFailure)
						{
							LogFailure(
								"Queue Processor",
								string.Format(
									"Failed to process Queue Item {0}, failure number {1}, item has been moved to failed item queue.{2}Exception Details:{2}{3}",
									item.Id,
									item.FailedAttempts,
									Environment.NewLine,
									e));
							_queue.StoreFailedMessage(item);
							_queue.DeleteQueueItem(item);
						}
						else
						{
							LogFailure(
								"Queue Processor",
								string.Format(
									"Failed to process Queue Item {0}, failure number {1}, will retry {2} times. {3}Exception Details:{3}{4}",
									item.Id,
									item.FailedAttempts,
									_retriesBeforeFailure - item.FailedAttempts,
									Environment.NewLine,
									e));
						}
					}
				}
			}
		}
	}
}
