import {
  bscWithdrawDepositProcess,
  bscPendingWithdrawalProcess,
  bscTxStatusUpdateProcess,
  bsc_blocks_process,
  bsc_tx_status_deposit_withdraw,
  bsc_pending_balance_update,
} from "./processes/index.process";
import schedule from "node-schedule";
class BSCStartProcess {
  constructor() {
    this.read_block_process();
    this.read_behined_block_process();
    this.readSpecificBlock();
    this.consume_read_block_queue();
    this.bscWithdrawDepositProcess();
    this.getPendingWithdrawalTxProcess();
    this.updateTxStatusProcess();
    this.BSCPendingBalanceUpdate();
    this.BSCUpdateBalanceByWallet();
    // this.testing_node()
  }
  public read_block_process() {
    schedule.scheduleJob("* * * * * *", async function () {
      await bsc_blocks_process.getBlocks();
    });
  }
  public read_behined_block_process() {
    schedule.scheduleJob("*/2 * * * * *", async function () {
      await bsc_blocks_process.readBehindBlock();
    });
  }
  // public testing_node() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await bsc_blocks_process.testingNode();
  //   });
  // }
  public readSpecificBlock() {
    schedule.scheduleJob("*/1 * * * *", async function () {
      await bsc_blocks_process.readSpecificBlock();
    });
  }
  public consume_read_block_queue() {
    setTimeout(async () => {
      console.log("consume_read_block_queue >>>>>>");
      await bsc_tx_status_deposit_withdraw.startBlockTxQueue();
      await bsc_tx_status_deposit_withdraw.startBehindBlockTxQueue();
      // await bsc_tx_status_deposit_withdraw.consume_queue_trnx();
    }, 5000);
  }
  public async bscWithdrawDepositProcess() {
    setTimeout(async () => {
      await bscWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }
  public getPendingWithdrawalTxProcess() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await bscPendingWithdrawalProcess.getTransactionFromDB();
    });
  }
  public async updateTxStatusProcess() {
    setTimeout(async () => {
      await bscTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }

  public async BSCPendingBalanceUpdate() {
    setTimeout(async () => {
      await bsc_pending_balance_update.default.update_wallet_balance();
    }, 10000);
  }

  public async BSCUpdateBalanceByWallet() {
    schedule.scheduleJob("*/2 * * * *", async function () {
      await bsc_pending_balance_update.default.updateWalletBalanceByWallet();
    });
  }
}

new BSCStartProcess();
