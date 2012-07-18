using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace SimpleSqlQueue
{
	[Serializable]
	public class QueueUnavailableException : Exception
	{
		public QueueUnavailableException()
		{
		}

		public QueueUnavailableException(string message) : base(message)
		{
		}

		public QueueUnavailableException(string message, Exception inner) : base(message, inner)
		{
		}

		protected QueueUnavailableException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}
