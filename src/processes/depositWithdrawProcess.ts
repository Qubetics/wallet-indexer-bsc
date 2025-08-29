import { TxType, WebHookTxType } from "../types/ERC20Tokens.types";
import { CoinInterface, NotificationInterface, TrnxHistoryModel } from "../models";
import { config } from "../config/config";
import bscProcessHelper from "./process.helper";
import { BlockChainStatusEnum, NotificationTypeEnum, TrnxTypeEnum, TxReqTypesEnum } from "../enum";
import { utilityHelper, walletHelper } from "../helpers"
import { Op } from "sequelize";
import { globalHelper } from "../helpers/global.helper";
import WalletModel from "../models/tables/model.wallets";
import kafkaHelper from "../config/kafka.helper";


class DepositWithdrawProcess {
  public bsc_deposit_withdraw_group_id = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_GROUP_ID;
  public bsc_deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

  public startDepositWithdrawConsumer = async () => {
    let consumer: any = null;
    let isConnected = false;

    const connectConsumer = async () => {
      try {

        if (consumer) {
          await consumer.disconnect();
        }

        consumer = await kafkaHelper.kafka.consumer({
          groupId: this.bsc_deposit_withdraw_group_id,
          // Add session timeout for better error handling
          sessionTimeout: 30000,
          // Add heartbeat interval
          heartbeatInterval: 3000,
        });

        await consumer.subscribe({
          topic: this.bsc_deposit_withdraw_topic,
          fromBeginning: true, // ✅ Changed to false to prevent reprocessing
        });

        await consumer.run({
          // ✅ Add proper autoCommit configuration
          autoCommit: true,
          autoCommitInterval: 5000,
          autoCommitThreshold: 100,

          eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
            try {
              console.log('🚀 ~ startDepositWithdrawConsumer topic', topic);
              console.log('🚀 ~ startDepositWithdrawConsumer message.value', message.value.toString());
              await this.checkIfOurTx(JSON.parse(message.value.toString()));

            } catch (error) {
              console.error(`❌ Error processing message:`, error);
            }
          },
        });
        isConnected = true;
      } catch (error) {
        console.error(`❌ Failed to connect consumer:`, error);
        isConnected = false;
        throw error;
      }
    };

    // ✅ Add reconnection logic
    const startWithReconnection = async () => {
      while (true) {
        try {
          if (!isConnected) {
            console.log(`🔄 Attempting to connect consumer startDepositQueue...`);
            await connectConsumer();
          }

          // Keep the consumer alive
          await new Promise(resolve => setTimeout(resolve, 30000));

        } catch (error) {
          console.error(`❌ Consumer connection error:`, error);
          isConnected = false;

          // Wait before retrying
          console.log(`⏳ Waiting 10 seconds before reconnecting...`);
          await new Promise(resolve => setTimeout(resolve, 10000));
        }
      }
    };

    // Start the consumer with reconnection
    await startWithReconnection();
  };

  private checkIfOurTx = async (data: TxType) => {
    if (data.amount == 0) {
      console.log("amount === 0")
    } else {
      console.log("amount is more than 0", data)
      let coin_data: any = null;
      if (data.token) {
        console.log("token trnx")
        coin_data = data.token;
      } else if (data.isNFTToken) {
        coin_data = data.isNFTToken;
      } else {
        console.log("coin trnx")
        coin_data = await bscProcessHelper.checkIfContract('NativeCoin');
      }
      const webhookTx: WebHookTxType = {
        coin: coin_data,
        tx_id: data.txId,
        to_address: data.toAddress,
        from_address: data.fromAddress,
        block_id: data.blockId,
        amount: data.amount,
        isNFTToken: data.isNFTToken || false,
        status: 'completed',
        blockchain_status: BlockChainStatusEnum.CONFIRMED,
        txType: data.txType,
        trnx_fee: data.trnx_fee
      };
      console.log(webhookTx, "DEPOSIT");
      await this.saveNewTrnx(webhookTx);
    }

  };

  private saveNewTrnx = async (
    trnx_data: WebHookTxType
  ) => {
    try {
      const tx_hash: string = trnx_data.tx_id;
      const to_address: string = trnx_data.to_address;
      const from_address: string = trnx_data.from_address;
      const coin: CoinInterface = trnx_data.coin;
      const amount: number = trnx_data.amount;
      const status: string = trnx_data.status;
      const block_number: number = trnx_data.block_id;
      const blockchain_status: BlockChainStatusEnum = trnx_data.blockchain_status;
      const trnx_fee: any = trnx_data.trnx_fee ? trnx_data.trnx_fee : 0;
      let tx_type: TrnxTypeEnum = trnx_data.txType ? trnx_data.txType : TrnxTypeEnum.DEPOSIT;
      const fiat_currency = "USD";
      let checkOldTrnx: any = null;
      if (tx_type == TrnxTypeEnum.WITHDRAW) {
        checkOldTrnx = await TrnxHistoryModel.findOne({
          attributes: ["coin_id", "tx_id", "type"],
          where: {
            tx_id: {
              [Op.like]: tx_hash,
            },
            from_adrs: {
              [Op.like]: from_address,
            },
            coin_id: coin.coin_id,
            req_type: { [Op.notIn]: [TxReqTypesEnum.ALCHEMY, TxReqTypesEnum.TRANSAK] }
          },
          raw: true,
        });
        if (checkOldTrnx == null) {
          const checkCoinInWallet: any = await WalletModel.findOne({
            attributes: ["coin_id", "coin_family"],
            where: {
              wallet_address: from_address,
              coin_id: coin.coin_id
            },
            raw: true
          })
          if (!checkCoinInWallet) {
            console.error(`Coin not exist under this wallet.`);
            return false;
          }
        }
      } else if (tx_type == TrnxTypeEnum.DEPOSIT) {
        checkOldTrnx = await TrnxHistoryModel.findOne({
          attributes: ["coin_id", "tx_id", "type"],
          where: {
            tx_id: {
              [Op.like]: tx_hash,
            },
            to_adrs: {
              [Op.like]: to_address,
            },
            coin_id: coin.coin_id,
            req_type: { [Op.notIn]: [TxReqTypesEnum.ALCHEMY, TxReqTypesEnum.TRANSAK] }
          },
          raw: true,
          logging: true
        });
      }

      if (checkOldTrnx) {
        console.error(`Transaction ${tx_hash} is already exists.`);
        return false;
      }

      if (tx_type !== TrnxTypeEnum.WITHDRAW && checkOldTrnx && checkOldTrnx.tx_id == tx_hash) {
        tx_type = checkOldTrnx.type;
      }
      const AddressWithdraw: any = await bscProcessHelper.check_our_wallet_address(from_address);
      const Addressdeposit: any = await bscProcessHelper.check_our_wallet_address(to_address);


      const user_id: number = AddressWithdraw ? AddressWithdraw.user_id : 0;
      const to_user_id: number = Addressdeposit ? Addressdeposit.user_id : 0;

      const trx_data: any = {
        user_id: user_id,
        to_user_id: to_user_id,
        req_type: TxReqTypesEnum.EXNG,
        from_adrs: from_address,
        to_adrs: to_address,
        coin_id: coin.coin_id,
        coin_family: coin.coin_family,
        amount: amount,
        status: status,
        block_id: block_number,
        tx_id: tx_hash,
        blockchain_status: blockchain_status,
        type: tx_type,
        tx_fee: trnx_fee,
        fiat_type: fiat_currency
      }
      const newTrnx: any = await TrnxHistoryModel.create(trx_data);

      if (Addressdeposit) {
        const trnxTypeD: string = "Deposit";
        /** update to adrs wallet balance */
        const balance: any = await walletHelper.updateBalance(
          to_address, /// wallet_address,
          coin /// coin
        );
        await globalHelper.checkCoinInWallet(to_address, coin, balance)
        const notiMsg = `${trnxTypeD} of ${await utilityHelper.bigNumberSafeConversion(
          amount
        )} ${coin?.coin_symbol?.toUpperCase()} has been confirmed.`;

        const notifData: NotificationInterface = {
          title: NotificationTypeEnum.DEPOSIT?.toUpperCase(),
          message: notiMsg,
          amount: amount,
          from_user_id: user_id,
          to_user_id: to_user_id,
          wallet_address: to_address,
          tx_id: newTrnx.id,
          coin_symbol: coin.coin_symbol,
          coin_id: coin.coin_id,
          coin_family: trx_data?.coin_family,
          tx_type: tx_type,
          notification_type: trnxTypeD,
          state: "confirmed"
        };
        utilityHelper.SendNotification(notifData);
      }

      if (AddressWithdraw) {
        let trnxTypeW: string = "Withdraw";

        switch (tx_type) {
          case TrnxTypeEnum.SMARTCONTRACTINTERACTION:
            trnxTypeW = "Smart Contract Interaction";
            break;
          case TrnxTypeEnum.APPROVE:
            trnxTypeW = "Approval";
            break;
          case TrnxTypeEnum.SWAP:
            trnxTypeW = "Swap";
            break;
          case TrnxTypeEnum.CROSS_CHAIN:
            trnxTypeW = "Cross Chain Swap";
            break;
          default:
            break;
        }

        await walletHelper.updateBalance(
          from_address, /// wallet_address,
          coin /// coin
        );
        if (coin.is_token) {
          const native_coin: any =
            await bscProcessHelper.checkIfContract('NativeCoin');
          await walletHelper.updateBalance(
            from_address, /// wallet_address,
            native_coin /// coin
          );
        }


        const notiMsg = `${trnxTypeW} of ${await utilityHelper.bigNumberSafeConversion(
          amount
        )} ${coin.coin_symbol?.toUpperCase()} has been confirmed.`;

        const notifData: NotificationInterface = {
          title: trnxTypeW?.toUpperCase(),
          amount: amount,
          message: notiMsg,
          from_user_id: 0,
          to_user_id: user_id,
          wallet_address: from_address,
          tx_id: newTrnx.id,
          tx_type: tx_type,
          coin_symbol: coin.coin_symbol,
          coin_id: coin.coin_id,
          coin_family: trx_data?.coin_family,
          notification_type: trnxTypeW,
          state: "confirmed"
        };

        utilityHelper.SendNotification(notifData);
      }

      return true;
    } catch (error: any) {
      console.error(`SaveTrnx_Process-saveNewTrnx error >>>`, error);
      return false;
    }
  };

}
export const depositWithdrawProcess = new DepositWithdrawProcess();
