﻿using Dapper;
using Elite_life_datacontext.DataBase;
using EliteLife2024_Worker.Model;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace EliteLife2024_Worker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                WriteToFile("Service is running at: " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                try
                {
                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        // 1. Gọi hàm dbo.get_order() để lấy dữ liệu
                        var connectPostgres = new ConnectToPostgresql(_configuration);
                        using var connection = await connectPostgres.CreateConnectionAsync();

                        using var command = connection.CreateCommand();
                        command.CommandText = @"SELECT * FROM dbo.get_order();";
                        using var reader = await command.ExecuteReaderAsync(stoppingToken);

                        if (await reader.ReadAsync(stoppingToken))
                        {
                            var collaboratorId = reader.GetInt32(0);
                            // Ánh xạ dữ liệu từ hàm get_order vào model
                            var order = new CommissionModel
                            {
                                CollaboratorId = reader.GetInt32(0),
                                AmountOrder = reader.GetInt32(1)
                            };

                            var gratitude = new GratitudeCommissionModel
                            {
                                CollaboratorId = reader.GetInt32(0),
                                AmountOrder = reader.GetInt32(1),
                                OrderId = reader.GetInt32(2)
                            };

                            var historyParam = new CreateWalletHistory
                            {
                                CollaboratorId = reader.GetInt32(0),
                                WalletType = "Source",
                                Value = ((reader.GetInt32(3)) * (-1)),
                                Note = $"Mua {reader.GetInt32(1)} combo",
                            };

                            //3. Cập nhật Rank
                            var rankResult = await CheckRankAncestorsAsync(collaboratorId);
                            _logger.LogInformation($"Kết quả kiểm tra rank: {rankResult}");
                            WriteToFile($"Kết quả kiểm tra rank: {rankResult}");

                            //4. Tạo lịch sử
                            var historyResult = await CreateWalletHistoryAsync(historyParam);

                            //5. Cập nhật Star
                            var starResult = await CheckStarAncestorsAsync(collaboratorId);
                            _logger.LogInformation($"Kết quả kiểm tra star: {starResult}");
                            WriteToFile($"Kết quả kiểm tra star: {starResult}");


                            // 2. Gọi các hàm chia hoa hồng
                            var shareResult = await CaculateShareCommissionAsync(order);
                            _logger.LogInformation($"Kết quả chia hoa hồng đồng chia: {shareResult}");
                            WriteToFile($"Kết quả chia hoa hồng đồng chia: {shareResult}");

                            var introResult = await CaculateIntroCommissionAsync(order);
                            _logger.LogInformation($"Kết quả chia hoa hồng giới thiệu: {introResult}");
                            WriteToFile($"Kết quả chia hoa hồng giới thiệu: {introResult}");

                            var leaderResult = await CaculateLeaderCommissionAsync(order);
                            _logger.LogInformation($"Kết quả chia hoa hồng lãnh đạo: {leaderResult}");
                            WriteToFile($"Kết quả chia hoa hồng lãnh đạo: {leaderResult}");

                            var gratitudeResult = await CaculateGratitudeCommissionAsync(gratitude);
                            _logger.LogInformation($"Kết quả chia hoa hồng tri ân: {gratitudeResult}");
                            WriteToFile($"Kết quả chia hoa hồng tri ân: {gratitudeResult}");


                            //6. Cập nhật IsProcess cho Order
                            var orderStatusResult = await UpdateOrderStatusAsync(collaboratorId);
                            _logger.LogInformation($"Kết quả kiểm tra status: {orderStatusResult}");
                            WriteToFile($"Kết quả kiểm tra status: {orderStatusResult}");

                        }
                        else
                        {
                            _logger.LogInformation("Không có bản ghi nào phù hợp từ hàm dbo.get_order().");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Lỗi khi xử lý: {ex.Message}");
                }
                // sau 1s nó chạy lại 1 lần
                await Task.Delay(10000, stoppingToken);
            }
        }
        // Log
        //public void WriteToFile(string Message)
        //{
        //    string path = AppDomain.CurrentDomain.BaseDirectory + "/Logs";
        //    if (!Directory.Exists(path))
        //    {
        //        Directory.CreateDirectory(path);
        //    }
        //    string filepath = AppDomain.CurrentDomain.BaseDirectory + "/Logs/ServiceLog_" + DateTime.Now.ToString("yyyy_MM_dd") + ".txt";
        //    if (!File.Exists(filepath))
        //    {
        //        // Create a file to write to.
        //        using (StreamWriter sw = File.CreateText(filepath))
        //        {
        //            sw.WriteLine(Message);
        //        }
        //    }
        //    else
        //    {
        //        using (StreamWriter sw = File.AppendText(filepath))
        //        {
        //            sw.WriteLine(Message);
        //        }
        //    }
        //}

        public void WriteToFile(string Message)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "\\Logs";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "\\Logs\\ServiceLog_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
            if (!File.Exists(filepath))
            {
                // Create a file to write to.
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
        }

        // Đồng chia
        public async Task<string> CaculateShareCommissionAsync(CommissionModel shareCommissionModel)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = @"SELECT * FROM dbo.cal_share_commission(
                     @p_collaboratorId, 
                     @p_amountOrder 
                     )";

                command.Parameters.AddWithValue("@p_collaboratorId", shareCommissionModel.CollaboratorId);
                command.Parameters.AddWithValue("@p_amountOrder", shareCommissionModel.AmountOrder);

                var result = (string)await command.ExecuteScalarAsync();
                return result;

            }
            catch (Exception ex)
            {
                return $"Lỗi khi xử lý: {ex.Message}";
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Giới thiệu
        public async Task<string> CaculateIntroCommissionAsync(CommissionModel introCommissionModel)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = @"SELECT * FROM dbo.cal_intro_commission(
                    @p_collaboratorid, 
                    @p_amountOrder 
                    )";

                command.Parameters.AddWithValue("@p_collaboratorId", introCommissionModel.CollaboratorId);
                command.Parameters.AddWithValue("@p_amountOrder", introCommissionModel.AmountOrder);

                var result = (string)await command.ExecuteScalarAsync();
                return result;

            }
            catch (Exception ex)
            {
                return $"Lỗi khi xử lý: {ex.Message}";
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Lãnh đạo
        public async Task<string> CaculateLeaderCommissionAsync(CommissionModel introCommissionModel)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = @"SELECT * FROM dbo.cal_leader_commission(
                    @p_collaboratorid, 
                    @p_amountOrder 
                    )";

                command.Parameters.AddWithValue("@p_collaboratorId", introCommissionModel.CollaboratorId);
                command.Parameters.AddWithValue("@p_amountOrder", introCommissionModel.AmountOrder);

                var result = (string)await command.ExecuteScalarAsync();
                return result;

            }
            catch (Exception ex)
            {
                return $"Lỗi khi xử lý: {ex.Message}";
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Tri ân
        public async Task<string> CaculateGratitudeCommissionAsync(GratitudeCommissionModel gratitudeCommissionModel)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            var maxLevel = 21;
            List<int> listOrdersId = new();
            for (int i = 0; i < maxLevel; i++)
            {
                if (gratitudeCommissionModel.OrderId % 2 != 0)
                {
                    gratitudeCommissionModel.OrderId = (gratitudeCommissionModel.OrderId - 1) / 2;
                }
                else
                {
                    gratitudeCommissionModel.OrderId = gratitudeCommissionModel.OrderId / 2;
                }

                if (gratitudeCommissionModel.OrderId >= 1)
                {
                    listOrdersId.Add(gratitudeCommissionModel.OrderId);
                }
                else
                {
                    break;
                }
            }

            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = @"SELECT * FROM dbo.cal_gratitude_commission(
                    @p_collaboratorId,
                    @p_amountOrder,
                    @p_ordersId)";

                command.Parameters.AddWithValue("@p_collaboratorId", gratitudeCommissionModel.CollaboratorId);
                command.Parameters.AddWithValue("@p_amountOrder", gratitudeCommissionModel.AmountOrder);
                command.Parameters.AddWithValue("@p_ordersId", listOrdersId.ToArray());

                var result = (string)await command.ExecuteScalarAsync();
                return result;
            }
            catch (Exception ex)
            {
                return $"Lỗi khi xử lý: {ex.Message}";
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Cập nhật rank cho 1 thằng cha
        public async Task<string> CheckRankAsync(int collaboratorId)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                var query = @"Select * from dbo.check_rank (@collaboratorId)";
                var result = await connection.ExecuteScalarAsync<string>(query, new { CollaboratorId = collaboratorId });

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi: {ex.Message}", ex);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Cập nhật rank cho tất cả cha, ông
        public async Task<string> CheckRankAncestorsAsync(int collaboratorId)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                var query = @"Select * from dbo.get_ancestors (@collaboratorId)";
                var ancestors = await connection.QueryAsync<int>(query, new { CollaboratorId = collaboratorId });
                ancestors = ancestors.Append(collaboratorId);
                foreach (var ancestorId in ancestors)
                {
                    await CheckRankAsync(ancestorId);
                }
                return "Đã cập nhật rank cho các bậc cha.";

            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi: {ex.Message}", ex);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Cập nhật star cho 1 thằng cha
        public async Task<string> CheckStarAsync(int collaboratorId)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                var query = @"Select * from dbo.check_star (@collaboratorId)";
                var result = await connection.ExecuteScalarAsync<string>(query, new { CollaboratorId = collaboratorId });

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi: {ex.Message}", ex);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Cập nhật star cho tất cả cha, ông

        public async Task<string> CheckStarAncestorsAsync(int collaboratorId)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                var query = @"Select * from dbo.get_ancestors (@collaboratorId)";
                var ancestors = await connection.QueryAsync<int>(query, new { CollaboratorId = collaboratorId });
                ancestors = ancestors.Append(collaboratorId);
                foreach (var ancestorId in ancestors)
                {
                    await CheckStarAsync(ancestorId);
                }
                return "Đã cập nhật sao cho các bậc cha.";

            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi: {ex.Message}", ex);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Cập nhật order status
        public async Task<string> UpdateOrderStatusAsync(int collaboratorId)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                var query = @"Select * from dbo.update_status_order (@collaboratorId)";
                var result = await connection.ExecuteScalarAsync<string>(query, new { CollaboratorId = collaboratorId });

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi: {ex.Message}", ex);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Tạo lịch sử
        public async Task<string> CreateWalletHistoryAsync(CreateWalletHistory createWalletHistory)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = @"SELECT * FROM dbo.create_wallet_history(
                    @p_collaboratorId, 
                    @p_walletType, 
                    @p_value,
                    @p_note)";

                command.Parameters.AddWithValue("@p_collaboratorId", createWalletHistory.CollaboratorId);
                command.Parameters.AddWithValue("@p_walletType", createWalletHistory.WalletType);
                command.Parameters.AddWithValue("@p_value", createWalletHistory.Value);
                command.Parameters.AddWithValue("@p_note", createWalletHistory.Note);

                await command.ExecuteNonQueryAsync();
                return "Lịch sử ví đã được tạo thành công.";
            }
            catch (Exception ex)
            {
                return $"Lỗi khi xử lý: {ex.Message}";
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

    }
}
