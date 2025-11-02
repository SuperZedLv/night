// 新的任务调度器 - 基于整点周期的任务管理

// 配置参数（可通过环境变量覆盖）
export const CONFIG = {
  MAX_ACTIVE_MINING: parseInt(process.env.MAX_ACTIVE_MINING) || 6,
  MAX_OPEN_PAGES: parseInt(process.env.MAX_OPEN_PAGES) || 12,
  PAGE_OPEN_CONCURRENCY: parseInt(process.env.PAGE_OPEN_CONCURRENCY) || 4,
  STATUS_CHECK_INTERVAL: parseInt(process.env.STATUS_CHECK_INTERVAL) || 5000, // 5秒检查一次状态
  COMPLETION_WAIT_TIME: 30000, // 任务完成后等待30秒再关闭
};

// 任务状态
export const TaskStatus = {
  PENDING: 'pending',        // 待执行
  INITIALIZING: 'initializing', // 初始化中（从点击enter address到start session）
  MINING: 'mining',          // 正在挖矿中（页面显示"finding a solution"，任务正在进行）
  COMPLETED: 'completed',    // 已完成（页面显示"waiting for the next challenge"，任务已完成）
  WAITING_CLOSE: 'waiting_close', // 等待关闭（已完成后等待30s）
  CLOSED: 'closed',          // 已关闭
  ERROR: 'error',            // 错误
};

class TaskScheduler {
  constructor() {
    this.tasks = new Map(); // taskId -> taskInfo
    this.isRunning = false;
    this.intervalId = null;
    this.currentCycleStartTime = null; // 当前周期的开始时间
    
    // 统计信息
    this.stats = {
      cycle: 0,
      totalCompleted: 0,
      cycleCompleted: 0,
      // ⚠️ 详细统计（与 runbatch.mjs 保持一致）
      success: 0,           // 成功完成的任务数
      failed: 0,            // 失败的任务数
      loggingIn: 0,         // 登录阶段：页面已打开但还未到达start session页面
      loggedIn: 0,          // 已登录状态：已到达start session页面但还未点击start按钮
      miningStarted: 0,     // 已开始挖矿的任务数（状态为"finding a solution"）
      loginTimes: [],       // 登录时间数组（从打开页面到start session页面的时间，单位：秒）
      miningTimes: [],      // 挖矿时间数组（从点击start session到状态变成waiting的时间，单位：秒）
      taskTimers: new Map(), // 每个任务的时间记录 { taskId: { pageOpenTime, loginCompleteTime, miningStartTime } }
    };
  }

  // 添加任务
  addTask(taskId, taskData) {
    if (this.tasks.has(taskId)) {
      console.warn(`[SCHEDULER] Task ${taskId} already exists`);
      return;
    }

    this.tasks.set(taskId, {
      id: taskId,
      addr: taskData.addr,
      status: TaskStatus.PENDING,
      page: null,
      browser: null, // 使用共享的browser context
      createdAt: Date.now(),
      completedAt: null,
      error: null,
      completionWaitStart: null, // 开始等待关闭的时间
    });

    console.log(`[SCHEDULER] Added task ${taskId} (total: ${this.tasks.size})`);
  }

  // 获取当前打开的页面数
  getOpenPagesCount() {
    let count = 0;
    for (const task of this.tasks.values()) {
      if (task.page && !task.page.isClosed() && 
          task.status !== TaskStatus.CLOSED && 
          task.status !== TaskStatus.ERROR) {
        count++;
      }
    }
    return count;
  }

  // 获取当前正在挖矿的任务数
  // ⚠️ 只统计状态为MINING的任务（页面显示"finding a solution"，任务正在进行中）
  getActiveMiningCount() {
    let count = 0;
    for (const task of this.tasks.values()) {
      // 状态为MINING表示任务正在进行中（页面显示"finding a solution"）
      if (task.status === TaskStatus.MINING && task.page && !task.page.isClosed()) {
        count++;
      }
    }
    return count;
  }

  // 检测任务状态
  async detectTaskStatus(taskId) {
    const task = this.tasks.get(taskId);
    if (!task || !task.page || task.page.isClosed()) {
      return { status: TaskStatus.CLOSED };
    }

    try {
      const page = task.page;
      await page.waitForTimeout(500);

      // 检查是否在挖矿页面
      const url = page.url();
      
      // 如果页面还在 wallet 页面，说明还在初始化阶段
      if (url.includes('/wizard/wallet')) {
        // 检查是否卡在 "Choose a Destination address" 页面
        const isStuck = await page.evaluate(() => {
          const bodyText = (document.body?.innerText || '').toLowerCase();
          return bodyText.includes('choose a destination address') || 
                 bodyText.includes('choose a destination');
        }).catch(() => false);
        
        if (isStuck) {
          console.warn(`[SCHEDULER] ⚠️ Task ${taskId} stuck on "Choose a Destination address" page, should click "Enter an address manually"`);
          // 返回 INITIALIZING 状态，让 runOne 继续处理
          return { status: TaskStatus.INITIALIZING };
        }
        
        // 如果不在挖矿页面，可能是初始化阶段
        if (task.status === TaskStatus.INITIALIZING || task.status === TaskStatus.PENDING) {
          return { status: TaskStatus.INITIALIZING };
        }
        return { status: TaskStatus.PENDING };
      }
      
      // 如果不在挖矿页面且不在wallet页面，可能是其他错误页面
      if (!url.includes('/wizard/mine')) {
        if (task.status === TaskStatus.INITIALIZING) {
          return { status: TaskStatus.INITIALIZING };
        }
        return { status: TaskStatus.PENDING };
      }

      // 检测页面状态
      // ⚠️ 增加等待时间，确保页面内容已完全渲染
      await page.waitForTimeout(1000);
      
      const statusInfo = await page.evaluate(() => {
        // 获取页面所有文本内容（包括隐藏元素）
        const bodyText = (document.body?.innerText || '').toLowerCase();
        const bodyHTML = (document.body?.innerHTML || '').toLowerCase();
        const allText = bodyText + ' ' + bodyHTML;
        
        // ⚠️ 在"Solve cryptographic challenges"页面检测状态
        // 状态显示位置通常在页面上的状态文本中
        // - "waiting for the next challenge" = 任务已完成
        // - "finding a solution" = 任务正在进行中（挖矿中）
        let challengeStatus = null;
        
        // 优先检测"waiting for the next challenge"（已完成状态）
        if (allText.includes('waiting for the next challenge')) {
          challengeStatus = 'waiting for the next challenge'; // ✅ 任务已完成
        } 
        // 然后检测"finding a solution"（正在进行中状态）
        else if (allText.includes('finding a solution')) {
          challengeStatus = 'finding a solution'; // ⛏️ 任务正在进行中（挖矿中）
        }
        // 兼容其他可能的"finding"文本（但要排除"finding"单独出现的情况，避免误判）
        else if (allText.includes('finding') && (allText.includes('solution') || allText.includes('challenge'))) {
          challengeStatus = 'finding a solution'; // ⛏️ 任务正在进行中
        }

        // 检测 start session 按钮
        const buttons = Array.from(document.querySelectorAll('button'));
        let hasStartSession = false;
        let hasStopSession = false;
        for (const btn of buttons) {
          const text = (btn.textContent || '').trim().toLowerCase();
          if ((text === 'start' || text === 'start session') && btn.offsetParent !== null && !btn.disabled) {
            hasStartSession = true;
          }
          if ((text === 'stop' || text === 'stop session') && btn.offsetParent !== null && !btn.disabled) {
            hasStopSession = true;
          }
        }

        return { challengeStatus, hasStartSession, hasStopSession, sampleText: bodyText.substring(0, 200) };
      });

      // ⚠️ 根据"Solve cryptographic challenges"页面的状态判断任务状态
      // 状态映射规则（根据用户要求）：
      // - "finding a solution" → MINING (任务正在进行中，挖矿中)
      // - "waiting for the next challenge" → COMPLETED (任务已完成)
      if (statusInfo.challengeStatus === 'waiting for the next challenge') {
        // ✅ 状态显示为"waiting for the next challenge"，任务已完成
        return { status: TaskStatus.COMPLETED };
      } else if (statusInfo.challengeStatus === 'finding a solution') {
        // ⛏️ 状态显示为"finding a solution"，任务正在进行中
        return { status: TaskStatus.MINING };
      } else if (statusInfo.hasStopSession) {
        // ⚠️ 有stop session按钮但没有状态文本，可能是正在挖矿（页面刚加载，状态文本还没更新）
        // 如果之前是MINING状态或INITIALIZING状态，保持或更新为MINING
        if (task.status === TaskStatus.MINING || task.status === TaskStatus.INITIALIZING) {
          return { status: TaskStatus.MINING };
        }
      } else if (statusInfo.hasStartSession) {
        // 有start session按钮，但还没有开始挖矿，处于初始化阶段
        return { status: TaskStatus.INITIALIZING };
      }

      // ⚠️ 如果没有检测到状态，但任务已经在MINING状态，保持MINING（避免误判）
      if (task.status === TaskStatus.MINING) {
        return { status: TaskStatus.MINING };
      }

      return { status: task.status }; // 保持当前状态
    } catch (error) {
      console.error(`[SCHEDULER] Error detecting status for task ${taskId}: ${error.message}`);
      return { status: TaskStatus.ERROR, error: error.message };
    }
  }

  // 点击Stop Session按钮
  // ⚠️ 用户要求：当超过MAX_ACTIVE_MINING限制时，点击stop session让任务回到start session状态
  async clickStopSession(taskId) {
    const task = this.tasks.get(taskId);
    if (!task) {
      console.error(`[SCHEDULER] Task ${taskId} not found`);
      return false;
    }

    if (!task.page || task.page.isClosed()) {
      console.warn(`[SCHEDULER] ⚠️ Task ${taskId} page not available`);
      return false;
    }

    try {
      const page = task.page;
      await page.waitForTimeout(500);
      
      // 查找Stop按钮并点击
      const clicked = await page.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll('button'));
        for (const btn of buttons) {
          const text = (btn.textContent || '').trim().toLowerCase();
          if ((text === 'stop' || text === 'stop session') && btn.offsetParent !== null && !btn.disabled) {
            btn.scrollIntoView({ behavior: 'smooth', block: 'center' });
            btn.click();
            return true;
          }
        }
        return false;
      }).catch(() => false);

      if (clicked) {
        console.log(`[SCHEDULER] 🛑 Stop session clicked for task ${taskId}, waiting for status update...`);
        await page.waitForTimeout(2000); // 等待状态更新（从"Finding a solution"回到"start session"状态）
        
        // 验证状态是否已更新（页面应该显示start session按钮）
        const hasStartButton = await page.evaluate(() => {
          const buttons = Array.from(document.querySelectorAll('button'));
          return buttons.some(btn => {
            const text = (btn.textContent || '').trim().toLowerCase();
            return (text === 'start' || text === 'start session') && btn.offsetParent !== null && !btn.disabled;
          });
        }).catch(() => false);
        
        if (hasStartButton) {
          console.log(`[SCHEDULER] ✅ Task ${taskId} successfully stopped, now showing start session button`);
          return true;
        } else {
          console.warn(`[SCHEDULER] ⚠️ Task ${taskId} stop clicked but start button not found yet`);
          return true; // 仍然返回true，可能状态更新需要更多时间
        }
      } else {
        console.warn(`[SCHEDULER] ⚠️ Stop button not found or not clickable for task ${taskId}`);
        return false;
      }
    } catch (error) {
      console.error(`[SCHEDULER] Error clicking stop for task ${taskId}: ${error.message}`);
      return false;
    }
  }

  // 执行任务初始化（从点击enter address到start session）
  async initializeTask(taskId) {
    const task = this.tasks.get(taskId);
    if (!task) {
      return false;
    }

    try {
      // 从runbatch.mjs导入任务执行函数
      const { runOne } = await import('./runbatch.mjs');
      
      // 执行初始化流程（initOnly=true，完成到start session按钮出现但不点击）
      // 创建一个临时调度器适配器，用于接收页面和浏览器
      const adapter = {
        tasks: new Map(),
        addTask: (id, page, browser) => {
          // 保存页面和浏览器引用
          task.page = page;
          task.browser = browser;
          this.tasks.set(id, task); // 确保任务已注册
        }
      };

      // ⚠️ 记录任务开始时间（页面打开时间），任务进入"登录阶段"
      if (!this.stats.taskTimers.has(taskId)) {
        this.stats.taskTimers.set(taskId, { pageOpenTime: Date.now() });
      } else {
        this.stats.taskTimers.get(taskId).pageOpenTime = Date.now();
      }
      this.stats.loggingIn++;
      
      const result = await runOne({ id: task.id, addr: task.addr }, { 
        initOnly: true, // 只完成到start session按钮出现
        scheduler: adapter 
      });

      if (result && result.ok && task.page) {
        // ⚠️ 检查是否已到达start session页面（已登录状态）
        // 已登录状态的定义：页面显示出"Solve cryptographic challenges"且页面里包含start session或stop session按钮
        const isLoggedInPage = await task.page.evaluate(() => {
          const bodyText = (document.body?.innerText || '').toLowerCase();
          const hasSolveCryptoText = bodyText.includes('solve cryptographic challenges');
          
          if (!hasSolveCryptoText) {
            return false;
          }
          
          const allButtons = Array.from(document.querySelectorAll('button'));
          const hasStartButton = allButtons.some(b => {
            const text = b.textContent?.trim().toLowerCase();
            return (text === 'start' || text === 'start session') && b.offsetParent !== null && !b.disabled;
          });
          const hasStopButton = allButtons.some(b => {
            const text = b.textContent?.trim().toLowerCase();
            return (text === 'stop' || text === 'stop session') && b.offsetParent !== null && !b.disabled;
          });
          
          return hasStartButton || hasStopButton;
        }).catch(() => false);
        
        if (isLoggedInPage) {
          // 已到达start session页面，从"登录阶段"转为"已登录状态"
          if (this.stats.loggingIn > 0) {
            this.stats.loggingIn--;
          }
          this.stats.loggedIn++;
          
          // ⚠️ 记录登录完成时间（到达start session页面的时间）
          const timer = this.stats.taskTimers.get(taskId);
          if (timer && timer.pageOpenTime) {
            timer.loginCompleteTime = Date.now();
            const loginTime = (timer.loginCompleteTime - timer.pageOpenTime) / 1000; // 转换为秒
            this.stats.loginTimes.push(loginTime);
          }
        }
        
        // 更新任务状态
        task.status = TaskStatus.INITIALIZING;
        console.log(`[SCHEDULER] ✅ Task ${taskId} initialized, page ready for start session`);
        return true;
      } else {
        // 初始化失败，清理统计
        if (this.stats.loggingIn > 0) {
          this.stats.loggingIn--;
        }
        this.stats.taskTimers.delete(taskId);
        this.stats.failed++;
        
        task.status = TaskStatus.ERROR;
        task.error = result?.error || 'Initialization failed';
        return false;
      }
    } catch (error) {
      console.error(`[SCHEDULER] Error initializing task ${taskId}: ${error.message}`);
      task.status = TaskStatus.ERROR;
      task.error = error.message;
      return false;
    }
  }

  // 关闭任务页面
  async closeTask(taskId) {
    const task = this.tasks.get(taskId);
    if (!task) {
      return;
    }

    try {
      // 关闭页面
      if (task.page && !task.page.isClosed()) {
        await task.page.close().catch(() => {});
      }
      task.page = null;
      
      // 关闭浏览器（如果这是该浏览器唯一/最后一个页面）
      // 注意：每个任务有自己的浏览器，所以可以直接关闭
      if (task.browser) {
        await task.browser.close().catch(() => {});
      }
      task.browser = null;
      
      task.status = TaskStatus.CLOSED;
      console.log(`[SCHEDULER] ✅ Closed task ${taskId}`);
    } catch (error) {
      console.error(`[SCHEDULER] Error closing task ${taskId}: ${error.message}`);
    }
  }

  // 检查是否到了新的周期（整点）
  checkCycleReset() {
    const now = new Date();
    const currentHour = now.getHours();
    
    if (!this.currentCycleStartTime) {
      // 第一次运行，设置当前周期
      this.currentCycleStartTime = new Date(now.getFullYear(), now.getMonth(), now.getDate(), currentHour, 0, 0);
      return false;
    }

    const cycleHour = this.currentCycleStartTime.getHours();
    
    // 如果当前小时与周期开始小时不同，说明进入了新周期
    if (currentHour !== cycleHour) {
      console.log(`[SCHEDULER] ⏰ New cycle detected: ${cycleHour}:00 -> ${currentHour}:00`);
      return true;
    }

    return false;
  }

  // 重置周期（重新开始所有任务）
  async resetCycle() {
    console.log(`[SCHEDULER] 🔄 Resetting cycle ${this.stats.cycle} -> ${this.stats.cycle + 1}`);
    
    // 关闭所有页面
    for (const taskId of this.tasks.keys()) {
      await this.closeTask(taskId);
    }

    // ⚠️ 清理统计信息（周期重置时需要清理状态计数，但保留累计统计）
    // 清理状态计数（这些是当前周期的实时计数）
    this.stats.loggingIn = 0;
    this.stats.loggedIn = 0;
    // 注意：不重置miningStarted和success，因为这些是累计统计
    // 但在关闭任务时，需要清理这些任务的计时器
    this.stats.taskTimers.clear();
    
    // 重置所有任务状态
    for (const task of this.tasks.values()) {
      task.status = TaskStatus.PENDING;
      task.page = null;
      task.completedAt = null;
      task.error = null;
      task.completionWaitStart = null;
    }

    // 更新周期信息
    const now = new Date();
    const currentHour = now.getHours();
    this.currentCycleStartTime = new Date(now.getFullYear(), now.getMonth(), now.getDate(), currentHour, 0, 0);
    this.stats.cycle++;
    this.stats.cycleCompleted = 0;
    
    console.log(`[SCHEDULER] ✅ Cycle reset complete. Starting cycle ${this.stats.cycle}`);
  }

  // 主调度循环
  async schedule() {
    if (!this.isRunning) {
      return;
    }

    // 检查周期重置
    if (this.checkCycleReset()) {
      await this.resetCycle();
    }

    const openPages = this.getOpenPagesCount();
    const activeMining = this.getActiveMiningCount();
    const totalTasks = this.tasks.size;

    // 统计待处理任务
    const pendingTasks = Array.from(this.tasks.values()).filter(t => 
      t.status === TaskStatus.PENDING
    );
    const initializingTasks = Array.from(this.tasks.values()).filter(t => 
      t.status === TaskStatus.INITIALIZING
    );
    const miningTasks = Array.from(this.tasks.values()).filter(t => 
      t.status === TaskStatus.MINING
    );
    const completedTasks = Array.from(this.tasks.values()).filter(t => 
      t.status === TaskStatus.COMPLETED || t.status === TaskStatus.WAITING_CLOSE
    );

    // ⚠️ 统一日志格式：所有地方都使用相同的计数方法
    console.log(`[SCHEDULER] 📊 Status: Pending=${pendingTasks.length}, Initializing=${initializingTasks.length}, Mining=${miningTasks.length}, Completed=${completedTasks.length}, OpenPages=${openPages}/${totalTasks}, ActiveMining=${activeMining}/${CONFIG.MAX_ACTIVE_MINING}${activeMining > CONFIG.MAX_ACTIVE_MINING ? ' ⚠️ EXCEEDED!' : ''}`);

    // 1. 处理已完成的任务（等待30秒后关闭）
    for (const task of completedTasks) {
      if (task.status === TaskStatus.COMPLETED) {
        // 开始等待（只在第一次设置）
        if (!task.completionWaitStart) {
          task.status = TaskStatus.WAITING_CLOSE;
          task.completionWaitStart = Date.now();
          console.log(`[SCHEDULER] ✅ Task ${task.id} completed, waiting 30s before close...`);
        }
      } else if (task.status === TaskStatus.WAITING_CLOSE) {
        // 检查是否等待了30秒
        if (!task.completionWaitStart) {
          // 如果没有设置等待开始时间，立即设置
          task.completionWaitStart = Date.now();
          continue;
        }
        
        const waitTime = Date.now() - task.completionWaitStart;
        if (waitTime >= CONFIG.COMPLETION_WAIT_TIME) {
          await this.closeTask(task.id);
          this.stats.totalCompleted++;
          this.stats.cycleCompleted++;
          
          // ⚠️ 清理任务时间记录（如果还在）
          this.stats.taskTimers.delete(task.id);
          
          console.log(`[SCHEDULER] ✅ Task ${task.id} closed after completion (waited ${Math.floor(waitTime/1000)}s)`);
        } else {
          // 输出剩余等待时间（每10秒输出一次）
          const remaining = Math.ceil((CONFIG.COMPLETION_WAIT_TIME - waitTime) / 1000);
          if (remaining % 10 === 0 || remaining <= 5) {
            console.log(`[SCHEDULER] ⏳ Task ${task.id} waiting to close, ${remaining}s remaining...`);
          }
        }
      }
    }

    // 2. 更新所有任务状态
    // ⚠️ 注意：在更新状态时，如果发现active mining超出限制，需要关闭部分任务
    for (const taskId of this.tasks.keys()) {
      const task = this.tasks.get(taskId);
      if (task.page && !task.page.isClosed()) {
        // ⚠️ 如果任务已经在等待关闭，不再更新状态（避免重置等待时间）
        if (task.status === TaskStatus.WAITING_CLOSE) {
          continue;
        }
        
        const detectedStatus = await this.detectTaskStatus(taskId);
        if (detectedStatus.status !== task.status) {
          const oldStatus = task.status;
          
          // ⚠️ 如果检测到状态变为MINING，检查是否会超出限制
          if (detectedStatus.status === TaskStatus.MINING) {
            const currentActiveMining = this.getActiveMiningCount();
            if (currentActiveMining >= CONFIG.MAX_ACTIVE_MINING) {
              // 已达到最大挖矿数，不更新为MINING状态
              console.warn(`[SCHEDULER] ⚠️ Task ${task.id} would start mining but limit reached (${currentActiveMining}/${CONFIG.MAX_ACTIVE_MINING}), keeping status as ${oldStatus}`);
              continue; // 保持当前状态，不更新为MINING
            }
          }
          
          task.status = detectedStatus.status;
          
          // ⚠️ 更新统计信息
          if (detectedStatus.status === TaskStatus.COMPLETED) {
            // ⚠️ 特殊情况：如果任务从INITIALIZING直接变为COMPLETED，说明页面已经处于完成状态
            // 但任务还没有经过挖矿阶段（没有点击start按钮），这可能是页面之前的状态
            // 我们应该先检查任务是否已经点击过start按钮（通过检查miningStartTime）
            const timer = this.stats.taskTimers.get(task.id);
            const hasStartedMining = timer && timer.miningStartTime;
            
            if (oldStatus === TaskStatus.INITIALIZING && !hasStartedMining) {
              // 任务还没有点击start按钮，不应该直接标记为完成
              // 保持INITIALIZING状态，等待调度器点击start按钮
              console.warn(`[SCHEDULER] ⚠️ Task ${task.id} shows "waiting for the next challenge" but hasn't started mining yet (still in INITIALIZING), ignoring completion status`);
              task.status = TaskStatus.INITIALIZING; // 恢复为INITIALIZING状态
              continue; // 跳过状态更新
            }
            
            // 状态变为COMPLETED：页面显示"waiting for the next challenge"，任务已完成
            console.log(`[SCHEDULER] ✅ Task ${task.id} completed (${oldStatus} -> ${detectedStatus.status}) [waiting for the next challenge]`);
            
            // ⚠️ 更新成功统计并记录挖矿完成时间
            this.stats.success++;
            
            // ⚠️ 记录挖矿完成时间（状态变成waiting for the next challenge的时间）
            if (timer && timer.miningStartTime) {
              const miningCompleteTime = Date.now();
              const miningTime = (miningCompleteTime - timer.miningStartTime) / 1000; // 转换为秒
              this.stats.miningTimes.push(miningTime);
            }
            
            // ⚠️ 清理状态计数（任务已完成）
            if (oldStatus === TaskStatus.MINING && this.stats.miningStarted > 0) {
              this.stats.miningStarted--;
            } else if (oldStatus === TaskStatus.INITIALIZING && this.stats.loggedIn > 0) {
              // 如果从INITIALIZING直接完成，也需要清理loggedIn计数
              this.stats.loggedIn--;
            }
            // 清理任务时间记录
            this.stats.taskTimers.delete(task.id);
            
          } else if (detectedStatus.status === TaskStatus.MINING) {
            // 状态变为MINING：页面显示"finding a solution"，任务正在进行中
            console.log(`[SCHEDULER] ⛏️ Task ${task.id} started mining (${oldStatus} -> ${detectedStatus.status}) [finding a solution]`);
            
            // ⚠️ 从"已登录状态"转为"挖矿中"（如果之前是INITIALIZING状态）
            if (oldStatus === TaskStatus.INITIALIZING && this.stats.loggedIn > 0) {
              this.stats.loggedIn--;
            }
            
            // ⚠️ 更新挖矿开始统计（只在第一次变为MINING时计数，避免重复）
            // 检查是否已经记录过挖矿开始（通过检查miningStartTime是否已设置）
            const timer = this.stats.taskTimers.get(task.id);
            if (timer && !timer.miningStartTime) {
              // 如果还没有记录挖矿开始时间，现在记录（点击start按钮的时间或当前时间）
              timer.miningStartTime = Date.now();
            }
            // 只有在任务第一次进入MINING状态时才增加计数
            if (oldStatus !== TaskStatus.MINING) {
              this.stats.miningStarted++;
            }
            
          } else if (detectedStatus.status === TaskStatus.INITIALIZING && oldStatus === TaskStatus.PENDING) {
            // 状态变为INITIALIZING：任务开始初始化
            // 静默处理，避免日志过多
          }
        } else if (task.status === TaskStatus.MINING) {
          // ⚠️ 如果任务已经是MINING状态但没有被检测到，可能需要重新检测
          // 添加调试日志（仅在调试模式下）
          if (process.env.DEBUG_SCHEDULER === 'true') {
            console.log(`[SCHEDULER] 🔍 Task ${task.id} is MINING but status check returned same status`);
          }
        }
      }
    }
    
    // ⚠️ 检查并处理超出限制的情况（如果状态更新后超出限制）
    // ⚠️ 用户要求：当检测出超过MAX_ACTIVE_MINING限制时，点击"stop session"按钮
    // 让任务回到显示"start session"按钮的状态，等待其他任务完成后重新启动
    // ⚠️ 重要：需要实际检测页面内容，因为有些页面可能显示"Finding a solution"但状态还没更新
    const allTasksWithPages = Array.from(this.tasks.values())
      .filter(t => t.page && !t.page.isClosed());
    
    // 实际检测所有页面，找出真正显示"Finding a solution"的页面
    const actuallyMiningTasks = [];
    for (const task of allTasksWithPages) {
      try {
        const page = task.page;
        const url = page.url();
        
        // 只检查挖矿页面
        if (!url.includes('/wizard/mine')) {
          continue;
        }
        
        // 实际检测页面是否显示"Finding a solution"
        const isMining = await page.evaluate(() => {
          const bodyText = (document.body?.innerText || '').toLowerCase();
          const bodyHTML = (document.body?.innerHTML || '').toLowerCase();
          const allText = bodyText + ' ' + bodyHTML;
          
          // 检查是否有"finding a solution"文本
          if (allText.includes('finding a solution')) {
            return true;
          }
          
          // 检查是否有stop session按钮（表示正在挖矿）
          const buttons = Array.from(document.querySelectorAll('button'));
          const hasStopSession = buttons.some(btn => {
            const text = (btn.textContent || '').trim().toLowerCase();
            return (text === 'stop' || text === 'stop session') && btn.offsetParent !== null && !btn.disabled;
          });
          
          // 如果有stop按钮且没有"waiting for the next challenge"，则认为正在挖矿
          if (hasStopSession && !allText.includes('waiting for the next challenge')) {
            return true;
          }
          
          return false;
        }).catch(() => false);
        
        if (isMining) {
          actuallyMiningTasks.push(task);
        }
      } catch (error) {
        // 忽略检测错误，继续处理其他任务
      }
    }
    
    // 如果实际挖矿任务数超过限制，停止多余的任务
    if (actuallyMiningTasks.length > CONFIG.MAX_ACTIVE_MINING) {
      console.warn(`[SCHEDULER] ⚠️ Active mining exceeded limit (${actuallyMiningTasks.length}/${CONFIG.MAX_ACTIVE_MINING} pages showing "Finding a solution"), stopping excess tasks...`);
      
      // 按创建时间排序，停止最晚的任务（后启动的优先停止）
      actuallyMiningTasks.sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0));
      
      // 停止超出限制的任务
      const toStop = actuallyMiningTasks.slice(CONFIG.MAX_ACTIVE_MINING);
      for (const task of toStop) {
        console.log(`[SCHEDULER] 🛑 Stopping task ${task.id} to enforce active mining limit (clicking stop session)...`);
        const oldStatus = task.status; // 保存旧状态
        const stopped = await this.clickStopSession(task.id);
        if (stopped) {
          // 点击stop成功后，将任务状态改回INITIALIZING，等待后续重新启动
          task.status = TaskStatus.INITIALIZING;
          // ⚠️ 更新统计：减少miningStarted计数（如果之前是MINING状态）
          if (oldStatus === TaskStatus.MINING && this.stats.miningStarted > 0) {
            this.stats.miningStarted--;
          }
          // ⚠️ 增加loggedIn计数（因为现在处于start session页面但未点击start按钮的状态）
          this.stats.loggedIn++;
          // ⚠️ 清理miningStartTime（因为停止挖矿了）
          const timer = this.stats.taskTimers.get(task.id);
          if (timer) {
            timer.miningStartTime = null;
          }
        } else {
          // 如果点击stop失败，尝试关闭页面（降级处理）
          console.warn(`[SCHEDULER] ⚠️ Failed to stop task ${task.id}, closing instead...`);
          await this.closeTask(task.id);
        }
      }
    }

    // 3. 启动新任务（如果满足条件）
    // 限制：
    // - 打开的页面数 < tasks.length
    // - 打开的页面数 < MAX_OPEN_PAGES
    // - 同时初始化的任务数 < PAGE_OPEN_CONCURRENCY
    const initializingCount = initializingTasks.length;
    const canOpenMore = openPages < totalTasks && 
                       openPages < CONFIG.MAX_OPEN_PAGES &&
                       initializingCount < CONFIG.PAGE_OPEN_CONCURRENCY;
    
    if (canOpenMore && pendingTasks.length > 0) {
      // 启动新任务（限制并发数）
      const toStart = Math.min(
        CONFIG.PAGE_OPEN_CONCURRENCY - initializingCount,
        pendingTasks.length
      );

      for (let i = 0; i < toStart; i++) {
        const task = pendingTasks[i];
        if (task.status === TaskStatus.PENDING) {
          console.log(`[SCHEDULER] 🚀 Starting task ${task.id}...`);
          task.status = TaskStatus.INITIALIZING;
          // 异步启动，不阻塞
          this.initializeTask(task.id).catch(err => {
            console.error(`[SCHEDULER] Error starting task ${task.id}: ${err.message}`);
            // ⚠️ 清理统计（启动失败）
            const timer = this.stats.taskTimers.get(task.id);
            if (timer) {
              this.stats.taskTimers.delete(task.id);
            }
            if (this.stats.loggingIn > 0) {
              this.stats.loggingIn--;
            }
            this.stats.failed++;
            
            task.status = TaskStatus.ERROR;
            task.error = err.message;
          });
        }
      }
    }

    // 4. 处理初始化完成的任务，点击start session
    // ⚠️ 重要：按顺序处理，并在每次点击前重新检查active mining数量
    for (const task of initializingTasks) {
      if (!task.page || task.page.isClosed()) {
        continue;
      }

      try {
        const url = task.page.url();
        
        // 如果任务卡在 wallet 页面的 "Choose a Destination address"，需要重新初始化
        if (url.includes('/wizard/wallet')) {
          const isStuck = await task.page.evaluate(() => {
            const bodyText = (document.body?.innerText || '').toLowerCase();
            return bodyText.includes('choose a destination address') || 
                   bodyText.includes('choose a destination');
          }).catch(() => false);
          
          if (isStuck) {
            console.warn(`[SCHEDULER] ⚠️ Task ${task.id} stuck on "Choose a Destination address", retrying initialization...`);
            // 重置任务状态，重新初始化
            task.status = TaskStatus.PENDING;
            // 异步重新初始化
            this.initializeTask(task.id).catch(err => {
              console.error(`[SCHEDULER] Error re-initializing stuck task ${task.id}: ${err.message}`);
              // ⚠️ 清理统计（重新初始化失败）
              const timer = this.stats.taskTimers.get(task.id);
              if (timer) {
                this.stats.taskTimers.delete(task.id);
              }
              if (this.stats.loggingIn > 0) {
                this.stats.loggingIn--;
              }
              if (this.stats.loggedIn > 0) {
                this.stats.loggedIn--;
              }
              this.stats.failed++;
              
              task.status = TaskStatus.ERROR;
              task.error = err.message;
            });
            continue;
          }
        }

        // ⚠️ 关键：在点击start session之前，重新计算active mining数量（状态可能已经变化）
        const currentActiveMining = this.getActiveMiningCount();
        if (currentActiveMining >= CONFIG.MAX_ACTIVE_MINING) {
          // 已达到最大挖矿数，跳过
          continue;
        }

        // 检查是否在挖矿页面并且有start session按钮
        if (url.includes('/wizard/mine')) {
          // ⚠️ 检查页面状态：如果页面显示"waiting for the next challenge"，这是正常的
          // 在这种情况下，页面应该也有"start session"按钮，可以开始新的挖矿周期
          const pageStatus = await task.page.evaluate(() => {
            const bodyText = (document.body?.innerText || '').toLowerCase();
            const bodyHTML = (document.body?.innerHTML || '').toLowerCase();
            const allText = bodyText + ' ' + bodyHTML;
            return {
              hasWaitingForNextChallenge: allText.includes('waiting for the next challenge'),
              hasFindingSolution: allText.includes('finding a solution'),
            };
          }).catch(() => ({ hasWaitingForNextChallenge: false, hasFindingSolution: false }));
          
          // 如果页面显示"waiting for the next challenge"，这是正常的（页面完成了一个周期，可以开始新的）
          // 如果页面显示"finding a solution"，说明已经在挖矿中，不应该点击start
          if (pageStatus.hasFindingSolution) {
            // 页面已经在挖矿中，跳过（状态检测会处理）
            continue;
          }
          
          // 尝试找到并点击start session按钮
          const startButton = task.page.getByRole('button', { name: /^(start|start session)$/i }).first();
          const isVisible = await startButton.isVisible({ timeout: 2000 }).catch(() => false);
          
          if (isVisible) {
            // 检查按钮是否可用
            const isEnabled = await startButton.isEnabled().catch(() => false);
            if (isEnabled) {
              // ⚠️ 再次检查active mining数量（可能在检查按钮时又有任务完成了）
              const finalCheckActiveMining = this.getActiveMiningCount();
              if (finalCheckActiveMining >= CONFIG.MAX_ACTIVE_MINING) {
                console.log(`[SCHEDULER] ⚠️ Task ${task.id} ready but active mining limit reached (${finalCheckActiveMining}/${CONFIG.MAX_ACTIVE_MINING}), skipping...`);
                continue;
              }
              
              // 点击start session
              // ⚠️ 如果页面显示"waiting for the next challenge"，说明这是开始新周期的挖矿
              if (pageStatus.hasWaitingForNextChallenge) {
                console.log(`[SCHEDULER] 🎯 Clicking start session for task ${task.id} to start new mining cycle (page shows "waiting for the next challenge")... (active mining: ${finalCheckActiveMining}/${CONFIG.MAX_ACTIVE_MINING})`);
              } else {
                console.log(`[SCHEDULER] 🎯 Clicking start session for task ${task.id}... (active mining: ${finalCheckActiveMining}/${CONFIG.MAX_ACTIVE_MINING})`);
              }
              
              // ⚠️ 从"已登录状态"转为准备挖矿（点击start按钮后）
              if (this.stats.loggedIn > 0) {
                this.stats.loggedIn--;
              }
              
              // ⚠️ 记录挖矿开始时间（点击start按钮的时间）
              // 注意：这个时间会在状态变为 MINING 时使用
              const timer = this.stats.taskTimers.get(task.id);
              if (timer && !timer.miningStartTime) {
                timer.miningStartTime = Date.now();
              } else if (!timer) {
                // 如果timer不存在，创建一个
                this.stats.taskTimers.set(task.id, {
                  pageOpenTime: Date.now(),
                  miningStartTime: Date.now(),
                });
              }
              
              await startButton.click({ timeout: 5000 }).catch(err => {
                console.warn(`[SCHEDULER] Error clicking start button for task ${task.id}: ${err.message}`);
              });
              await task.page.waitForTimeout(2000); // 等待状态更新
              // 状态将在下一次检测时更新为MINING
              // 注意：miningStarted 计数和统计将在状态变为 MINING 时更新（避免重复计数）
            }
          }
        }
      } catch (error) {
        console.error(`[SCHEDULER] Error checking/starting task ${task.id}: ${error.message}`);
        // ⚠️ 清理统计（检查/启动任务时出错）
        const timer = this.stats.taskTimers.get(task.id);
        if (timer) {
          this.stats.taskTimers.delete(task.id);
        }
        if (this.stats.loggingIn > 0) {
          this.stats.loggingIn--;
        }
        if (this.stats.loggedIn > 0) {
          this.stats.loggedIn--;
        }
        // 检查任务是否在挖矿中
        if (task.status === TaskStatus.MINING && this.stats.miningStarted > 0) {
          this.stats.miningStarted--;
        }
        this.stats.failed++;
        
        task.status = TaskStatus.ERROR;
        task.error = error.message;
      }
    }
  }

  // 启动调度器
  async start() {
    if (this.isRunning) {
      console.warn('[SCHEDULER] Scheduler is already running');
      return;
    }

    this.isRunning = true;
    this.currentCycleStartTime = null; // 将在第一次schedule时设置

    // 启动调度循环
    this.intervalId = setInterval(() => {
      this.schedule().catch(err => {
        console.error(`[SCHEDULER] Error in schedule loop: ${err.message}`);
      });
    }, CONFIG.STATUS_CHECK_INTERVAL);

    console.log('[SCHEDULER] ✅ Scheduler started');
    
    // 立即执行一次
    await this.schedule();
  }

  // 停止调度器
  async stop() {
    if (!this.isRunning) {
      return;
    }

    this.isRunning = false;
    
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }

    // 关闭所有页面和浏览器
    for (const taskId of this.tasks.keys()) {
      await this.closeTask(taskId);
    }

    console.log('[SCHEDULER] ✅ Scheduler stopped');
  }

  // 获取状态信息
  getStatus() {
    // 计算平均登录时间和平均挖矿时间
    const avgLoginTime = this.stats.loginTimes.length > 0 
      ? (this.stats.loginTimes.reduce((sum, t) => sum + t, 0) / this.stats.loginTimes.length).toFixed(2)
      : '0.00';
    const avgMiningTime = this.stats.miningTimes.length > 0
      ? (this.stats.miningTimes.reduce((sum, t) => sum + t, 0) / this.stats.miningTimes.length).toFixed(2)
      : '0.00';
    
    // ⚠️ 当前正在挖矿的任务数应该直接使用getActiveMiningCount()（基于实际任务状态）
    // 而不是通过miningStarted - success计算，因为周期重置时统计可能不一致
    const activeMiningCount = this.getActiveMiningCount();
    
    const status = {
      isRunning: this.isRunning,
      cycle: this.stats.cycle,
      totalTasks: this.tasks.size,
      openPages: this.getOpenPagesCount(),
      activeMining: activeMiningCount,
      maxActiveMining: CONFIG.MAX_ACTIVE_MINING,
      maxOpenPages: CONFIG.MAX_OPEN_PAGES,
      // ⚠️ 详细统计（与 runbatch.mjs 保持一致）
      success: this.stats.success,
      failed: this.stats.failed,
      loggingIn: this.stats.loggingIn,
      loggedIn: this.stats.loggedIn,
      miningStarted: this.stats.miningStarted, // 累计开始挖矿的任务数
      currentlyMining: activeMiningCount, // 当前实际正在挖矿的任务数（与activeMining一致）
      avgLoginTime: avgLoginTime,
      avgMiningTime: avgMiningTime,
      loginTimesCount: this.stats.loginTimes.length,
      miningTimesCount: this.stats.miningTimes.length,
      tasks: {},
    };

    for (const [taskId, task] of this.tasks) {
      status.tasks[taskId] = {
        status: task.status,
        error: task.error,
        completedAt: task.completedAt,
      };
    }

    return status;
  }
}

export { TaskScheduler };
