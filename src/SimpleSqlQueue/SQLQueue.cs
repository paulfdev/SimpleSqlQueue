using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Transactions;

namespace SimpleSqlQueue
{
	public class SQLQueue<T> : IQueue<T>
		where T :class
	{
		#region Members
		private readonly string _connectionString;
		#endregion

		#region Constructor
		public SQLQueue(string connectionString)
		{
			_connectionString = connectionString;
		}

		#endregion

		#region Operations
		public void Enqueue(QueueItem<T> message)
		{
			using (var transaction = GetTransaction())
			{
				using (var connection = GetSqlConnection())
				{
					try
					{
						connection.Open();
						try
						{
							var command = GetEnqueueCommand(connection, message.Id, GetBytesFromMessage(message));
							command.ExecuteNonQuery();
							transaction.Complete();
						}
						finally
						{
							connection.Close();
						}
					}
					catch (SqlException e)
					{
						//throw new QueueNotAvailableException(e);
					}
				}
			}
		}

		public QueueItem<T> Dequeue(int visibilityTimeout)
		{
			using (var transaction = GetTransaction())
			{
				var connection = GetSqlConnection();
				try
				{
					connection.Open();
					try
					{
						using (var reader = GetDequeueCommand(connection, visibilityTimeout).ExecuteReader())
						{
							reader.Read();
							if (reader.HasRows)
							{
								var queueItem = new QueueItem<T>(reader.GetGuid(0)) {FailedAttempts = reader.GetInt32(1)};
								var byteArray = (byte[])reader[2];
								queueItem.Payload = GetMessageFromBytes(byteArray);
								queueItem.TimeStamp = reader.GetDateTime(3);
								transaction.Complete();
								return queueItem;
							}
						}
					}
					finally
					{
						connection.Close();
					}
				}
				catch (SqlException e)
				{
					//throw new QueueNotAvailableException(e);
				}
			}
			return default(QueueItem<T>);
		}

		public void DeleteQueueItem(QueueItem<T> id)
		{
			using (var transaction = GetTransaction())
			{
				var connection = GetSqlConnection();
				try
				{
					connection.Open();
					try
					{
						GetDeleteQueueItemCommand(connection, id.Id).ExecuteNonQuery();
						transaction.Complete();
					}
					finally
					{
						connection.Close();
					}
				}
				catch (SqlException e)
				{
					//throw new QueueNotAvailableException(e);
				}
			}
		}

		public void DeleteFailedQueueItem(QueueItem<T> id)
		{
			using (var transaction = GetTransaction())
			{
				var connection = GetSqlConnection();
				try
				{
					connection.Open();
					try
					{
						GetDeleteFailedQueueItemCommand(connection, id.Id).ExecuteNonQuery();
						transaction.Complete();
					}
					finally
					{
						connection.Close();
					}
				}
				catch (SqlException e)
				{
					//throw new QueueNotAvailableException(e);
				}
			}
		}

		public void StoreFailedMessage(QueueItem<T> item)
		{
			using (var transaction = GetTransaction())
			{
				using (var connection = GetSqlConnection())
				{
					try
					{
						connection.Open();
						try
						{
							var command = GetInsertFailedMessageCommand(connection, item.Id, GetBytesFromMessage(item));
							command.ExecuteNonQuery();
							transaction.Complete();
						}
						finally
						{
							connection.Close();
						}
					}
					catch (SqlException e)
					{
						//throw new QueueNotAvailableException(e);
					}
				}
			}
		}

		#endregion

		#region Serialization
		private static byte[] GetBytesFromMessage(QueueItem<T> queueItem)
		{
			var stream = new MemoryStream();
			var bf = new BinaryFormatter();
			bf.Serialize(stream, queueItem.Payload);
			stream.Position = 0;
			var buffer = new byte[stream.Length];
			stream.Read(buffer, 0, buffer.Length);
			stream.Close();
			return buffer;
		}

		private static T GetMessageFromBytes(byte[] bytes)
		{
			var bf = new BinaryFormatter();
			var memstream = new MemoryStream(bytes);
			var queueItem = (T)bf.Deserialize(memstream);
			return queueItem;
		}
		#endregion

		#region SQL Helpers
		private SqlConnection GetSqlConnection()
		{
			return new SqlConnection(_connectionString);
		}

		private static SqlCommand GetEnqueueCommand(SqlConnection connection, Guid itemId, byte[] byteArray)
		{
			var command = connection.CreateCommand();
			command.CommandText = "Enqueue";
			command.CommandType = CommandType.StoredProcedure;
			command.Parameters.AddWithValue("@MessageId", itemId);
			command.Parameters.AddWithValue("@payload", byteArray);
			return command;
		}

		private static SqlCommand GetDequeueCommand(SqlConnection connection, int visibilityTimeout)
		{
			var command = connection.CreateCommand();
			command.CommandText = "Dequeue";
			command.CommandType = CommandType.StoredProcedure;
			command.Parameters.AddWithValue("@VisibilityTimeout", visibilityTimeout);
			return command;
		}

		private static SqlCommand GetDeleteQueueItemCommand(SqlConnection connection, Guid id)
		{
			var command = connection.CreateCommand();
			command.CommandText = "DeleteQueueItem";
			command.CommandType = CommandType.StoredProcedure;
			command.Parameters.AddWithValue("@Id", id);
			return command;
		}

		private static SqlCommand GetInsertFailedMessageCommand(SqlConnection connection, Guid oldQueueId, byte[] payload)
		{
			var command = connection.CreateCommand();
			command.CommandText = "InsertNewFailedMessage";
			command.CommandType = CommandType.StoredProcedure;
			command.Parameters.AddWithValue("@Id", oldQueueId);
			command.Parameters.AddWithValue("@payload", payload);
			return command;
		}

		private static SqlCommand GetDeleteFailedQueueItemCommand(SqlConnection connection, Guid id)
		{
			var command = connection.CreateCommand();
			command.CommandText = "DeleteFailedQueueItem";
			command.CommandType = CommandType.StoredProcedure;
			command.Parameters.AddWithValue("@Id", id);
			return command;
		}

		public static TransactionScope GetTransaction()
		{
			return new TransactionScope(TransactionScopeOption.Required,
										new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted });
		}

		#endregion
	}
}
