// run-batch.js
import { chromium } from 'playwright';
import { readFileSync, statSync, readdirSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join, extname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const CONCURRENCY = parseInt(process.env.CONCURRENCY) || 5; // 并发数（可通过环境变量配置）
const TASK_TIMEOUT_MS = 120_000;
export const BASE_URL = 'https://sm.midnight.gd/wizard/mine'; // 目标网页

// 签名服务 URL（可通过环境变量配置）
export const SIGN_SERVICE_URL = process.env.SIGN_SERVICE_URL || 'https://as.lku3ogjddfkj2.shop';

// 通用重试机制函数
async function retryWithBackoff(operation, options = {}) {
  const {
    maxRetries = 3,
    initialDelay = 1000,
    maxDelay = 10000,
    backoffMultiplier = 2,
    retryCondition = () => true, // 返回 true 表示应该重试
    operationName = 'Operation'
  } = options;
  
  let lastError;
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const result = await operation();
      if (attempt > 0) {
        console.log(`[RETRY] ${operationName} succeeded on attempt ${attempt + 1}`);
      }
      return result;
    } catch (error) {
      lastError = error;
      const errorMsg = String(error);
      
      // 检查是否应该重试
      if (!retryCondition(error, attempt)) {
        throw error;
      }
      
      // 如果是最后一次尝试，直接抛出错误
      if (attempt === maxRetries - 1) {
        console.error(`[RETRY] ${operationName} failed after ${maxRetries} attempts:`, errorMsg);
        throw error;
      }
      
      // 计算等待时间（指数退避）
      const delay = Math.min(initialDelay * Math.pow(backoffMultiplier, attempt), maxDelay);
      console.log(`[RETRY] ${operationName} failed (attempt ${attempt + 1}/${maxRetries}): ${errorMsg.substring(0, 100)}... Retrying in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  throw lastError || new Error(`${operationName} failed after ${maxRetries} attempts`);
}

// 页面导航请求速率限制：全局队列，每次请求间隔至少 1 秒
// 使用 Promise 队列确保并发任务真正串行化
let lastPageRequestTime = 0;
const PAGE_REQUEST_MIN_INTERVAL = 2000; // 最小请求间隔（毫秒）- 增加到2秒，控制向 BASE_URL 的请求速率
let navigationQueue = Promise.resolve(); // 全局导航队列

// 等待页面稳定（没有正在进行的导航或DOM变化）
async function waitForPageStable(page, timeoutMs = 5000) {
  const startTime = Date.now();
  
  while (Date.now() - startTime < timeoutMs) {
    try {
      // 检查是否有正在进行的导航
      const isNavigating = await page.evaluate(() => {
        return document.readyState !== 'complete' || 
               (window.performance && window.performance.navigation && 
                window.performance.navigation.type === 1); // TYPE_RELOAD = 1
      }).catch(() => false);
      
      if (!isNavigating) {
        // 等待一小段时间，确认DOM稳定（没有快速变化）
        await page.waitForTimeout(300);
        
        // 再次检查DOM是否还在变化
        const domStable = await page.evaluate(() => {
          // 如果页面有正在进行的动画或过渡，可能还在变化
          const hasActiveAnimations = document.querySelectorAll('*').length > 0;
          return document.readyState === 'complete';
        }).catch(() => true);
        
        if (domStable) {
          return true; // 页面已稳定
        }
      }
    } catch (e) {
      // 如果检查失败，继续等待
    }
    
    await page.waitForTimeout(200);
  }
  
  return false; // 超时，但继续执行
}

// 检查元素是否稳定存在（多次确认，避免因用户交互导致临时消失）
async function waitForElementStable(page, selectorOrLocator, options = {}) {
  const { timeout = 10000, checkInterval = 500, minStableCount = 2 } = options;
  const startTime = Date.now();
  let stableCount = 0;
  
  while (Date.now() - startTime < timeout) {
    try {
      const isVisible = typeof selectorOrLocator === 'string' 
        ? await page.locator(selectorOrLocator).first().isVisible().catch(() => false)
        : await selectorOrLocator.isVisible().catch(() => false);
      
      if (isVisible) {
        stableCount++;
        if (stableCount >= minStableCount) {
          return true; // 元素稳定存在
        }
      } else {
        stableCount = 0; // 重置计数
      }
    } catch (e) {
      stableCount = 0;
    }
    
    await page.waitForTimeout(checkInterval);
  }
  
  return stableCount >= minStableCount;
}

// 安全点击（检测并处理可能的用户交互干扰）
async function safeClick(page, selectorOrLocator, options = {}) {
  const { timeout = 10000, force = true, retries = 3 } = options;
  
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      // 等待页面稳定
      await waitForPageStable(page, 2000);
      
      // 等待元素稳定
      const locator = typeof selectorOrLocator === 'string' 
        ? page.locator(selectorOrLocator).first()
        : selectorOrLocator;
      
      const isStable = await waitForElementStable(page, locator, { timeout: 5000 });
      if (!isStable) {
        console.warn(`[SAFE-CLICK] Element not stable, attempt ${attempt}/${retries}`);
        if (attempt < retries) {
          await page.waitForTimeout(1000);
          continue;
        }
      }
      
      // 执行点击（使用 force 选项，即使元素被其他元素覆盖也能点击）
      await locator.click({ timeout, force });
      
      // 点击后短暂等待，确认操作生效
      await page.waitForTimeout(300);
      
      return true;
    } catch (e) {
      console.warn(`[SAFE-CLICK] Attempt ${attempt}/${retries} failed: ${e.message}`);
      if (attempt < retries) {
        await page.waitForTimeout(1000 * attempt); // 递增等待时间
      } else {
        throw e;
      }
    }
  }
  
  return false;
}

// 检查页面是否被重定向到错误页面（429错误页面或其他错误页面）
async function checkIfErrorPage(page) {
  try {
    const currentUrl = page.url();
    // 检查是否是 429 错误页面
    if (currentUrl.includes('/error?code=429') || (currentUrl.includes('/error') && currentUrl.includes('429'))) {
      return { isErrorPage: true, errorCode: '429', url: currentUrl };
    }
    // 检查是否是 403 错误页面
    if (currentUrl.includes('/error?code=403') || (currentUrl.includes('/error') && currentUrl.includes('403'))) {
      return { isErrorPage: true, errorCode: '403', url: currentUrl };
    }
    // 检查页面内容中是否显示403错误
    try {
      const pageText = await page.textContent('body').catch(() => '');
      if (pageText && /403|forbidden/i.test(pageText)) {
        // 检查是否在错误页面元素中
        const errorElements = await page.locator('[role="alert"], .error, .error-message, [class*="error"]').all().catch(() => []);
        for (const el of errorElements) {
          const text = await el.textContent().catch(() => '');
          if (text && /403|forbidden/i.test(text)) {
            return { isErrorPage: true, errorCode: '403', url: currentUrl };
          }
        }
      }
    } catch (e) {
      // 忽略检查错误
    }
    // 检查是否是其他错误页面（如 /wizard/t-c）
    if (currentUrl.includes('/wizard/t-c') || currentUrl.includes('/error')) {
      return { isErrorPage: true, errorCode: 'other', url: currentUrl };
    }
    return { isErrorPage: false };
  } catch (e) {
    return { isErrorPage: false };
  }
}

// 通过 Reset session 重置页面到初始状态
async function resetSessionAndReturn(page) {
  try {
    console.log('[RESET-SESSION] Attempting to reset via Reset session button...');
    
    // 1. 查找 "Reset session" 按钮
    let resetBtn = null;
    
    // 策略1: 使用 getByRole 查找
    try {
      const resetBtns = page.getByRole('button', { name: /reset\s*session/i });
      const buttons = await resetBtns.all();
      for (const btn of buttons) {
        if (await btn.isVisible({ timeout: 2000 }).catch(() => false)) {
          resetBtn = btn;
          break;
        }
      }
    } catch {}
    
    // 策略2: 使用 locator 查找
    if (!resetBtn) {
      try {
        const resetLocator = page.locator('button').filter({ hasText: /reset\s*session/i }).first();
        if (await resetLocator.isVisible({ timeout: 3000 }).catch(() => false)) {
          resetBtn = resetLocator;
        }
      } catch {}
    }
    
    // 策略3: 通过页面评估查找
    if (!resetBtn) {
      const btnFound = await page.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll('button, a, [role="button"]'));
        const resetBtn = buttons.find(btn => {
          const text = (btn.textContent || '').toLowerCase();
          return text.includes('reset') && text.includes('session') && btn.offsetParent !== null;
        });
        if (resetBtn) {
          resetBtn.scrollIntoView({ behavior: 'smooth', block: 'center' });
          return true;
        }
        return false;
      }).catch(() => false);
      
      if (btnFound) {
        resetBtn = page.locator('button').filter({ hasText: /reset\s*session/i }).first();
      }
    }
    
    if (!resetBtn) {
      console.warn('[RESET-SESSION] Reset session button not found');
      return false;
    }
    
    // 2. 记录第一个按钮的位置/特征，然后点击它
    console.log('[RESET-SESSION] Clicking first Reset session button...');
    await waitForPageStable(page, 2000);
    
    // 获取第一个按钮的信息（用于后续排除）
    let firstBtnInfo = null;
    try {
      const firstBtnHandle = await resetBtn.elementHandle().catch(() => null);
      if (firstBtnHandle) {
        firstBtnInfo = await firstBtnHandle.evaluate((btn) => {
          const rect = btn.getBoundingClientRect();
          return {
            x: rect.x,
            y: rect.y,
            text: btn.textContent?.trim() || ''
          };
        }).catch(() => null);
      }
    } catch {}
    
    await safeClick(page, resetBtn, { timeout: 10000, force: true, retries: 3 });
    
    // 等待弹出窗口出现（最多等待3秒）
    console.log('[RESET-SESSION] Waiting for confirmation dialog to appear...');
    await page.waitForTimeout(1500);
    
    // 3. 查找并点击第二个 Reset session 按钮（确认对话框中的）
    console.log('[RESET-SESSION] Looking for confirmation Reset session button in dialog...');
    let confirmResetBtn = null;
    let confirmBtnFound = false;
    
    // 尝试多次查找确认按钮（因为弹出窗口可能需要时间）
    for (let attempt = 1; attempt <= 5; attempt++) {
      // 策略1: 查找所有 Reset session 按钮，排除第一个按钮
      try {
        const allResetBtns = page.getByRole('button', { name: /reset\s*session/i });
        const buttons = await allResetBtns.all();
        
        for (const btn of buttons) {
          if (await btn.isVisible({ timeout: 1000 }).catch(() => false)) {
            // 获取按钮位置信息
            const btnHandle = await btn.elementHandle().catch(() => null);
            if (btnHandle) {
              const btnInfo = await btnHandle.evaluate((b) => {
                const rect = b.getBoundingClientRect();
                return {
                  x: rect.x,
                  y: rect.y,
                  text: b.textContent?.trim() || ''
                };
              }).catch(() => null);
              
              // 如果位置不同，或者找不到第一个按钮的信息，就认为是确认按钮
              if (!firstBtnInfo || 
                  btnInfo && (btnInfo.x !== firstBtnInfo.x || btnInfo.y !== firstBtnInfo.y)) {
                confirmResetBtn = btn;
                confirmBtnFound = true;
                console.log('[RESET-SESSION] Found confirmation Reset session button (by position)');
                break;
              }
            }
          }
        }
      } catch {}
      
      // 策略2: 查找在对话框/模态框中的按钮（通常在更高层级）
      if (!confirmBtnFound) {
        try {
          // 查找可能的对话框或模态框
          const dialogBtns = page.locator('[role="dialog"] button, .modal button, [class*="dialog"] button, [class*="modal"] button').filter({ hasText: /reset\s*session/i });
          const dialogButton = await dialogBtns.first().isVisible({ timeout: 1000 }).catch(() => false);
          if (dialogButton) {
            confirmResetBtn = dialogBtns.first();
            confirmBtnFound = true;
            console.log('[RESET-SESSION] Found confirmation Reset session button (in dialog)');
            break;
          }
        } catch {}
      }
      
      // 策略3: 查找所有可见的 Reset session 按钮，选择第二个可见的
      if (!confirmBtnFound) {
        try {
          const allBtns = page.locator('button').filter({ hasText: /reset\s*session/i });
          const count = await allBtns.count();
          let visibleIndex = -1;
          
          for (let i = 0; i < count; i++) {
            const btn = allBtns.nth(i);
            if (await btn.isVisible({ timeout: 500 }).catch(() => false)) {
              visibleIndex++;
              // 第二个可见的按钮通常是确认按钮
              if (visibleIndex === 1) {
                confirmResetBtn = btn;
                confirmBtnFound = true;
                console.log('[RESET-SESSION] Found confirmation Reset session button (second visible button)');
                break;
              }
            }
          }
        } catch {}
      }
      
      // 策略4: 通过 evaluate 查找所有可见按钮，选择位置不同的那个
      if (!confirmBtnFound) {
        try {
          const btnInfo = await page.evaluate((firstBtnPos) => {
            const buttons = Array.from(document.querySelectorAll('button'));
            const resetButtons = buttons.filter(btn => {
              const text = (btn.textContent || '').toLowerCase();
              return text.includes('reset') && text.includes('session') && btn.offsetParent !== null;
            });
            
            if (resetButtons.length >= 2 && firstBtnPos) {
              // 找到位置不同的按钮
              for (const btn of resetButtons) {
                const rect = btn.getBoundingClientRect();
                if (rect.x !== firstBtnPos.x || rect.y !== firstBtnPos.y) {
                  return {
                    x: rect.x,
                    y: rect.y,
                    text: btn.textContent?.trim() || ''
                  };
                }
              }
            }
            return null;
          }, firstBtnInfo).catch(() => null);
          
          if (btnInfo) {
            // 使用文本和位置信息再次定位
            const confirmBtns = page.locator('button').filter({ hasText: /reset\s*session/i });
            const buttons = await confirmBtns.all();
            for (const btn of buttons) {
              if (await btn.isVisible({ timeout: 500 }).catch(() => false)) {
                const btnPos = await btn.elementHandle().then(h => 
                  h?.evaluate(b => ({ x: b.getBoundingClientRect().x, y: b.getBoundingClientRect().y })).catch(() => null)
                ).catch(() => null);
                
                if (btnPos && (btnPos.x !== firstBtnInfo?.x || btnPos.y !== firstBtnInfo?.y)) {
                  confirmResetBtn = btn;
                  confirmBtnFound = true;
                  console.log('[RESET-SESSION] Found confirmation Reset session button (by evaluate)');
                  break;
                }
              }
            }
          }
        } catch {}
      }
      
      if (confirmBtnFound) break;
      
      if (attempt < 5) {
        console.log(`[RESET-SESSION] Confirmation button not found yet, waiting... (attempt ${attempt}/5)`);
        await page.waitForTimeout(500);
      }
    }
    
    if (confirmResetBtn && confirmBtnFound) {
      console.log('[RESET-SESSION] Clicking confirmation Reset session button...');
      await waitForPageStable(page, 2000);
      await safeClick(page, confirmResetBtn, { timeout: 10000, force: true, retries: 3 });
      await page.waitForTimeout(2000);
    } else {
      // 如果没有找到第二个按钮，可能对话框已经自动关闭，或者按钮文本不同
      console.warn('[RESET-SESSION] Confirmation button not found after all attempts, checking if already reset...');
      // 检查是否已经回到初始状态
      const alreadyReset = await page.getByText('Enter an address manually', { exact: true }).isVisible({ timeout: 2000 }).catch(() => false);
      if (alreadyReset) {
        console.log('[RESET-SESSION] Already returned to initial state (no confirmation needed)');
        return true;
      }
      // 如果还没重置，等待一下看看是否会自动关闭
      await page.waitForTimeout(2000);
    }
    
    // 4. 等待页面回到初始状态
    console.log('[RESET-SESSION] Waiting for page to return to initial state...');
    await page.waitForTimeout(3000);
    
    // 5. 验证是否回到了初始状态（检查是否有 "Enter an address manually" 按钮）
    const isReset = await page.getByText('Enter an address manually', { exact: true }).isVisible({ timeout: 5000 }).catch(() => false);
    
    if (isReset) {
      console.log('[RESET-SESSION] Successfully reset to initial state via Reset session!');
      return true;
    } else {
      console.warn('[RESET-SESSION] Page may not be fully reset, but continuing...');
      return true;
    }
  } catch (e) {
    console.error(`[RESET-SESSION] Error during reset session: ${e.message}`);
    return false;
  }
}

// 通过 Disconnect 重置页面到初始状态
async function disconnectAndReset(page) {
  try {
    console.log('[DISCONNECT] Attempting to reset page via Disconnect...');
    
    // 1. 查找 "Scavenger Mine" 下面的 "Disconnect" 按钮
    let disconnectBtn = null;
    
    // 策略1: 查找包含 "Scavenger Mine" 文本附近的 Disconnect 按钮
    try {
      const scavengerMineText = page.getByText('Scavenger Mine', { exact: false });
      if (await scavengerMineText.first().isVisible({ timeout: 3000 }).catch(() => false)) {
        // 在 Scavenger Mine 的父容器或兄弟元素中查找 Disconnect
        const container = await scavengerMineText.first().locator('..').first().locator('..').first().elementHandle().catch(() => null);
        if (container) {
          const disconnectInContainer = page.locator('button, a, [role="button"]').filter({ hasText: /disconnect/i });
          const buttons = await disconnectInContainer.all();
          for (const btn of buttons) {
            if (await btn.isVisible({ timeout: 1000 }).catch(() => false)) {
              disconnectBtn = btn;
              break;
            }
          }
        }
      }
    } catch {}
    
    // 策略2: 直接查找所有 Disconnect 按钮，选择可见的第一个
    if (!disconnectBtn) {
      try {
        const allDisconnectBtns = page.getByRole('button', { name: /disconnect/i });
        const buttons = await allDisconnectBtns.all();
        for (const btn of buttons) {
          if (await btn.isVisible({ timeout: 1000 }).catch(() => false)) {
            disconnectBtn = btn;
            break;
          }
        }
      } catch {}
    }
    
    // 策略3: 使用 locator 查找包含 disconnect 文本的按钮
    if (!disconnectBtn) {
      try {
        const disconnectLocator = page.locator('button').filter({ hasText: /disconnect/i }).first();
        if (await disconnectLocator.isVisible({ timeout: 3000 }).catch(() => false)) {
          disconnectBtn = disconnectLocator;
        }
      } catch {}
    }
    
    if (!disconnectBtn) {
      console.warn('[DISCONNECT] Disconnect button not found, trying alternative methods...');
      // 尝试通过页面评估查找
      const btnFound = await page.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll('button, a, [role="button"]'));
        const disconnectBtn = buttons.find(btn => {
          const text = (btn.textContent || '').toLowerCase();
          return text.includes('disconnect') && btn.offsetParent !== null;
        });
        if (disconnectBtn) {
          disconnectBtn.scrollIntoView({ behavior: 'smooth', block: 'center' });
          return true;
        }
        return false;
      }).catch(() => false);
      
      if (!btnFound) {
        throw new Error('Disconnect button not found on page');
      }
      
      // 再次尝试查找
      disconnectBtn = page.locator('button').filter({ hasText: /disconnect/i }).first();
    }
    
    // 点击 Disconnect 按钮
    if (disconnectBtn) {
      console.log('[DISCONNECT] Clicking Disconnect button...');
      await disconnectBtn.scrollIntoViewIfNeeded();
      await disconnectBtn.click({ timeout: 5000 });
      await page.waitForTimeout(1500); // 等待对话框/子页面出现
    } else {
      throw new Error('Could not find Disconnect button');
    }
    
    // 2. 等待并处理弹出的对话框/子页面
    console.log('[DISCONNECT] Waiting for dialog/modal to appear...');
    await page.waitForTimeout(1000);
    
    // 在对话框/子页面中点击 Disconnect
    let dialogDisconnectClicked = false;
    try {
      // 查找对话框中的 Disconnect 按钮
      const dialogDisconnect = page.locator('button, [role="button"]').filter({ hasText: /^disconnect$/i }).first();
      if (await dialogDisconnect.isVisible({ timeout: 3000 }).catch(() => false)) {
        console.log('[DISCONNECT] Clicking Disconnect in dialog...');
        await dialogDisconnect.click({ timeout: 5000 });
        await page.waitForTimeout(1000);
        dialogDisconnectClicked = true;
      }
    } catch {}
    
    // 如果没找到，尝试在所有 frame 中查找
    if (!dialogDisconnectClicked) {
      try {
        for (const frame of page.frames()) {
          try {
            const frameDisconnect = frame.locator('button, [role="button"]').filter({ hasText: /^disconnect$/i }).first();
            if (await frameDisconnect.isVisible({ timeout: 2000 }).catch(() => false)) {
              console.log('[DISCONNECT] Clicking Disconnect in frame...');
              await frameDisconnect.click({ timeout: 5000 });
              await page.waitForTimeout(1000);
              dialogDisconnectClicked = true;
              break;
            }
          } catch {}
        }
      } catch {}
    }
    
    // 使用页面评估查找并点击 Disconnect
    if (!dialogDisconnectClicked) {
      const clicked = await page.evaluate(() => {
        // 查找所有按钮，包括可能在 shadow DOM 中的
        const allButtons = Array.from(document.querySelectorAll('button, [role="button"], a'));
        const disconnectBtn = allButtons.find(btn => {
          const text = (btn.textContent || '').trim().toLowerCase();
          return text === 'disconnect' && btn.offsetParent !== null;
        });
        
        if (disconnectBtn) {
          disconnectBtn.click();
          return true;
        }
        return false;
      }).catch(() => false);
      
      if (clicked) {
        console.log('[DISCONNECT] Clicked Disconnect via evaluate...');
        await page.waitForTimeout(1000);
        dialogDisconnectClicked = true;
      }
    }
    
    // 3. 点击 Close 按钮关闭对话框
    console.log('[DISCONNECT] Looking for Close button...');
    await page.waitForTimeout(500);
    
    let closeClicked = false;
    try {
      const closeBtn = page.getByRole('button', { name: /^close$/i }).first();
      if (await closeBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
        console.log('[DISCONNECT] Clicking Close button...');
        await closeBtn.click({ timeout: 5000 });
        await page.waitForTimeout(1000);
        closeClicked = true;
      }
    } catch {}
    
    // 如果没找到 Close，尝试其他方式
    if (!closeClicked) {
      try {
        const closeLocator = page.locator('button, [role="button"]').filter({ hasText: /^close$/i }).first();
        if (await closeLocator.isVisible({ timeout: 2000 }).catch(() => false)) {
          console.log('[DISCONNECT] Clicking Close via locator...');
          await closeLocator.click({ timeout: 5000 });
          await page.waitForTimeout(1000);
          closeClicked = true;
        }
      } catch {}
    }
    
    // 使用页面评估查找并点击 Close
    if (!closeClicked) {
      const clicked = await page.evaluate(() => {
        const allButtons = Array.from(document.querySelectorAll('button, [role="button"], [aria-label*="close" i]'));
        const closeBtn = allButtons.find(btn => {
          const text = (btn.textContent || '').trim().toLowerCase();
          const ariaLabel = (btn.getAttribute('aria-label') || '').toLowerCase();
          return (text === 'close' || ariaLabel.includes('close')) && btn.offsetParent !== null;
        });
        
        if (closeBtn) {
          closeBtn.click();
          return true;
        }
        return false;
      }).catch(() => false);
      
      if (clicked) {
        console.log('[DISCONNECT] Clicked Close via evaluate...');
        await page.waitForTimeout(1000);
        closeClicked = true;
      }
    }
    
    // 4. 等待页面回到初始状态
    console.log('[DISCONNECT] Waiting for page to reset to initial state...');
    await page.waitForTimeout(2000);
    
    // 5. 验证是否回到了初始状态（检查是否有 "Enter an address manually" 按钮）
    const isReset = await page.getByText('Enter an address manually', { exact: true }).isVisible({ timeout: 5000 }).catch(() => false);
    
    if (isReset) {
      console.log('[DISCONNECT] Successfully reset to initial state!');
      return true;
    } else {
      console.warn('[DISCONNECT] Page may not be fully reset, but continuing...');
      // 即使没有确认回到初始状态，也返回 true，让流程继续
      return true;
    }
  } catch (e) {
    console.error(`[DISCONNECT] Error during disconnect and reset: ${e.message}`);
    
    // 如果是在 429 错误页面且找不到 Disconnect 按钮，尝试直接导航回去
    if (e.message.includes('Disconnect button not found')) {
      try {
        console.log('[DISCONNECT] Disconnect button not found, attempting direct navigation to BASE_URL...');
        await page.goto(BASE_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
        await page.waitForTimeout(2000);
        console.log('[DISCONNECT] Direct navigation successful');
        return true;
      } catch (navError) {
        console.error(`[DISCONNECT] Direct navigation also failed: ${navError.message}`);
      }
    }
    
    return false;
  }
}

// 检查页面是否显示 "too many requests" 错误（只检查实际显示的错误消息，不检查API错误）
async function checkPageForRateLimitError(page, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      // 检查页面是否还在加载中，避免在导航时检查
      try {
        const currentUrl = page.url();
        const evalUrl = await page.evaluate(() => window.location.href).catch(() => '');
        if (currentUrl !== evalUrl) {
          await page.waitForLoadState('domcontentloaded', { timeout: 2000 }).catch(() => {});
        }
      } catch {}
      
      // 等待页面稳定
      await page.waitForTimeout(300);
      
      const checkResult = await page.evaluate(() => {
        // 只检查可见的错误消息元素，不检查页面文本中的"403"（可能是API错误）
        const errorSelectors = [
          '[role="alert"]',
          '.error',
          '.error-message',
          '[class*="error"]:not([class*="hidden"])',
          '[class*="rate-limit"]',
          '[class*="too-many"]',
          '[data-error]',
          '[aria-live="polite"][role="status"]',
          '[aria-live="assertive"]'
        ];
        
        let foundErrorText = null;
        
        // 首先检查专门的错误元素
        for (const selector of errorSelectors) {
          try {
            const els = document.querySelectorAll(selector);
            for (const el of els) {
              // 只检查可见的元素
              const style = window.getComputedStyle(el);
              if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                continue;
              }
              
              const text = el.textContent?.trim() || '';
              if (text.length === 0) continue;
              
              // 检查是否是速率限制相关的错误消息（排除API响应中的403）
              if (/you'?re making too many requests/i.test(text) ||
                  /too many requests.*please wait/i.test(text) ||
                  /please wait.*try again/i.test(text) ||
                  /rate limit.*exceeded/i.test(text) ||
                  /try again later/i.test(text)) {
                foundErrorText = text;
                break;
              }
            }
            if (foundErrorText) break;
          } catch {}
        }
        
        // 如果没有找到错误元素，检查页面主要区域是否有明确的错误消息
        if (!foundErrorText) {
          const mainContent = document.querySelector('main, [role="main"], .content, .container, body');
          if (mainContent) {
            const mainText = mainContent.innerText || '';
            // 只匹配完整的错误消息，不是简单的"403"数字
            const errorMatch = mainText.match(/(you'?re making too many requests[^.!]*[.!]|too many requests[^.!]*please wait[^.!]*[.!]|please wait.*moment.*try again[^.!]*[.!])/i);
            if (errorMatch) {
              foundErrorText = errorMatch[0].trim();
            }
          }
        }
        
        return foundErrorText;
      }).catch((e) => {
        // 如果页面正在导航或执行上下文被销毁，返回 null（非错误状态）
        if (e.message && (e.message.includes('Execution context was destroyed') || 
                          e.message.includes('navigation') ||
                          e.message.includes('Target closed'))) {
          return null;
        }
        // 其他错误也返回 null，避免误判
        return null;
      });
      
      if (checkResult) {
        return { hasError: true, errorText: checkResult };
      }
      
      return { hasError: false };
    } catch (e) {
      // 如果是因为页面导航导致的错误，不算作真正的错误，返回 false
      if (e.message && (e.message.includes('Execution context was destroyed') || 
                        e.message.includes('Target closed') ||
                        e.message.includes('navigation'))) {
        return { hasError: false };
      }
      
      if (i === retries - 1) {
        // 只在最后一次重试失败时才记录警告
        console.warn(`[RATE-LIMIT-CHECK] Failed to check page for rate limit error: ${e.message}`);
      }
      await page.waitForTimeout(500);
    }
  }
  
  return { hasError: false };
}

// 处理错误页面：检测并恢复（只有429错误页面使用Disconnect重置，其他错误页面直接导航回去）
async function handleErrorPage(page, targetUrl = BASE_URL) {
  const errorPageCheck = await checkIfErrorPage(page);
  if (!errorPageCheck.isErrorPage) {
    return false; // 不是错误页面
  }
  
  // 429 或 403 错误页面：使用 Disconnect 重置
  if (errorPageCheck.errorCode === '429' || errorPageCheck.errorCode === '403') {
    const errorType = errorPageCheck.errorCode === '429' ? '429' : '403';
    console.warn(`[${errorType}-ERROR] Detected ${errorType} error page at ${errorPageCheck.url}`);
    console.log(`[${errorType}-ERROR] Attempting to reset via Disconnect...`);
    
    // 429/403 错误页面需要使用 Disconnect 重置
    const reset = await disconnectAndReset(page);
    if (reset) {
      return true;
    }
    
    // 如果 Disconnect 失败，尝试直接导航
    console.warn(`[${errorType}-ERROR] Disconnect reset failed, trying direct navigation...`);
    try {
      await page.goto(targetUrl, {
        waitUntil: 'domcontentloaded',
        timeout: 30000
      });
      await page.waitForTimeout(2000);
      return true;
    } catch (e) {
      console.error(`[${errorType}-ERROR] Navigation also failed: ${e.message}`);
      return false;
    }
  }
  
  // 其他错误页面（如 /wizard/t-c）：直接导航回去，不使用 Disconnect
  console.warn(`[ERROR-PAGE] Detected error page (non-429/403) at ${errorPageCheck.url}`);
  console.log(`[ERROR-PAGE] Navigating back to ${targetUrl}...`);
  
  try {
    await page.goto(targetUrl, {
      waitUntil: 'domcontentloaded',
      timeout: 30000
    });
    await page.waitForTimeout(2000); // 等待页面加载
    
    // 检查是否仍然在错误页面
    const afterNavCheck = await checkIfErrorPage(page);
    if (afterNavCheck.isErrorPage) {
      console.warn(`[ERROR-PAGE] Still on error page after navigation, waiting longer...`);
      // 等待一段时间后再试导航
      await page.waitForTimeout(5000);
      await page.goto(targetUrl, {
        waitUntil: 'domcontentloaded',
        timeout: 30000
      });
      await page.waitForTimeout(2000);
    }
    
    console.log(`[ERROR-PAGE] Successfully navigated back to target page`);
    return true;
  } catch (e) {
    console.error(`[ERROR-PAGE] Failed to navigate back: ${e.message}`);
    return false;
  }
}

// 保持向后兼容的别名
async function handle429ErrorPage(page, targetUrl = BASE_URL) {
  return await handleErrorPage(page, targetUrl);
}

// 等待页面错误消失（限制最大等待时间，避免无限循环）
async function waitForRateLimitErrorToClear(page, maxWaitMs = 30000, checkIntervalMs = 3000, targetUrl = BASE_URL) {
  const startTime = Date.now();
  let consecutiveErrors = 0;
  const maxConsecutiveErrors = 5; // 最多连续5次检查到错误就放弃等待
  
  while (Date.now() - startTime < maxWaitMs) {
    // 首先检查是否是429错误页面
    const errorPageCheck = await checkIfErrorPage(page);
    if (errorPageCheck.isErrorPage && errorPageCheck.errorCode === '429') {
      console.warn(`[429-ERROR] Detected 429 error page, navigating back to ${targetUrl}...`);
      const navigated = await handleErrorPage(page, targetUrl);
      if (navigated) {
        // 导航成功后，检查页面错误消息
        await page.waitForTimeout(2000);
        const check = await checkPageForRateLimitError(page, 1);
        if (!check.hasError) {
          console.log(`[RATE-LIMIT] Error cleared after navigating back from 429 page`);
          return true;
        }
      }
    }
    
    // 检查页面错误消息
    const check = await checkPageForRateLimitError(page, 1);
    if (!check.hasError) {
      // 确认不在错误页面
      const stillOnErrorPage = await checkIfErrorPage(page);
      if (!stillOnErrorPage.isErrorPage) {
        console.log(`[RATE-LIMIT] Error cleared after ${Date.now() - startTime}ms`);
        return true; // 错误已消失
      }
    }
    
    consecutiveErrors++;
    if (consecutiveErrors >= maxConsecutiveErrors) {
      console.warn(`[RATE-LIMIT] Error persists after ${consecutiveErrors} checks, checking for 429 error page...`);
      
      // 最后检查是否是错误页面，如果是则处理
      const finalErrorPageCheck = await checkIfErrorPage(page);
      if (finalErrorPageCheck.isErrorPage) {
        console.log(`[ERROR-PAGE] Final attempt: handling error page (${finalErrorPageCheck.errorCode})...`);
        await handleErrorPage(page, targetUrl);
        await page.waitForTimeout(2000);
      } else {
        // 尝试刷新页面
        try {
          console.log(`[RATE-LIMIT] Attempting to refresh page...`);
          await page.reload({ waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => {});
          await page.waitForTimeout(2000);
          
          // 刷新后检查是否跳转到错误页面
          const afterRefreshErrorCheck = await checkIfErrorPage(page);
          if (afterRefreshErrorCheck.isErrorPage) {
            await handleErrorPage(page, targetUrl);
          }
          
          // 检查一次，如果还有错误就放弃
          const afterRefreshCheck = await checkPageForRateLimitError(page, 1);
          if (!afterRefreshCheck.hasError) {
            const finalErrorCheck = await checkIfErrorPage(page);
            if (!finalErrorCheck.isErrorPage) {
              console.log(`[RATE-LIMIT] Error cleared after refresh`);
              return true;
            }
          }
        } catch {}
      }
      
      // 即使刷新后还有错误，也继续执行，因为可能是API错误不影响页面功能
      console.warn(`[RATE-LIMIT] Continuing despite error (may be API-only error)...`);
      return false;
    }
    
    const remaining = Math.max(0, maxWaitMs - (Date.now() - startTime));
    const waitTime = Math.min(checkIntervalMs, remaining);
    if (waitTime > 0) {
      console.log(`[RATE-LIMIT] Rate limit error still present (${consecutiveErrors}/${maxConsecutiveErrors}), waiting ${waitTime}ms...`);
      await page.waitForTimeout(waitTime);
    } else {
      break;
    }
  }
  
  // 超时前最后检查错误页面
  const timeoutErrorPageCheck = await checkIfErrorPage(page);
  if (timeoutErrorPageCheck.isErrorPage) {
    console.log(`[ERROR-PAGE] Timeout detected error page (${timeoutErrorPageCheck.errorCode}), handling...`);
    await handleErrorPage(page, targetUrl);
  }
  
  console.warn(`[RATE-LIMIT] Wait timeout after ${Date.now() - startTime}ms, continuing...`);
  return false; // 超时，但继续执行
}

// 带速率限制和重试的页面导航函数（全局串行化）
async function gotoWithRateLimit(page, url, options = {}) {
  // 使用全局队列确保并发任务串行化
  return new Promise((resolve, reject) => {
    navigationQueue = navigationQueue.then(async () => {
      try {
        // 先进行速率限制
        const now = Date.now();
        const timeSinceLastRequest = now - lastPageRequestTime;
        
        if (timeSinceLastRequest < PAGE_REQUEST_MIN_INTERVAL) {
          const waitTime = PAGE_REQUEST_MIN_INTERVAL - timeSinceLastRequest;
          console.log(`[NAV] Rate limiting: waiting ${waitTime}ms before navigating to ${url}...`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
        }
        
        lastPageRequestTime = Date.now();
        
        // 使用重试机制执行页面导航
        const result = await retryWithBackoff(
          async () => {
            console.log(`[NAV] Navigating to ${url}...`);
  
            
            // 只监听主页面 URL 的响应错误（忽略 API 请求）
            let mainPageResponseError = null;
            const errorHandler = (response) => {
              if (!response) return;
              const responseUrl = response.url();
              // 只检查主页面 URL 的响应，忽略 API 请求
              const isMainPageUrl = responseUrl === url || responseUrl.startsWith(url.split('?')[0]);
              const isApiRequest = responseUrl.includes('/api/') || responseUrl !== url;
              
              // 只有主页面响应错误才应该阻止导航
              if (isMainPageUrl && !isApiRequest) {
                if (response.status() === 403) {
                  mainPageResponseError = { status: 403, url: responseUrl };
                } else if (response.status() === 429) {
                  mainPageResponseError = { status: 429, url: responseUrl };
                }
              }
            };
            
            page.on('response', errorHandler);
            
            try {
              // 在 headless 模式下使用更宽松的超时和等待策略
              const response = await page.goto(url, {
                waitUntil: 'domcontentloaded',
                timeout: 45000, // 增加超时时间，headless 模式下可能需要更长时间
                ...options
              }).catch(async (gotoError) => {
                // 如果是 ERR_ABORTED 错误，可能是某些资源加载失败，但页面可能已经加载了
                if (gotoError.message && gotoError.message.includes('ERR_ABORTED')) {
                  console.warn(`[NAV] Navigation aborted (ERR_ABORTED), but page may have loaded. Checking page state...`);
                  // 等待一下，然后检查页面是否实际加载了
                  await page.waitForTimeout(2000);
                  try {
                    const currentUrl = page.url();
                    // 如果 URL 已经改变，说明导航成功了（只是被中止）
                    if (currentUrl.includes(url.split('?')[0]) || currentUrl === url) {
                      console.log(`[NAV] Page actually loaded despite ERR_ABORTED, URL: ${currentUrl}`);
                      // 尝试获取响应（可能为 null）
                      return null; // 返回 null，后续检查会通过
                    }
                  } catch (checkError) {
                    // 如果检查失败，继续抛出原始错误
                  }
                }
                throw gotoError;
              });
              
              // 检查主页面响应状态（只有主页面响应才应该阻止导航）
              // 注意：response 可能为 null（如果导航被中止但页面已加载）
              if (response && !response.ok() && response.status() >= 400) {
                // 检查是否是速率限制或禁止访问错误
                if (response.status() === 429) {
                  throw new Error('Rate limit error (429): Too many requests');
                }
                if (response.status() === 403) {
                  throw new Error('Forbidden error (403): Access denied - possibly rate limited or blocked');
                }
                throw new Error(`Navigation failed with status ${response.status()}: ${response.statusText()}`);
              }
              
              // 检查主页面响应错误（API 403 不应该阻止导航）
              if (mainPageResponseError) {
                if (mainPageResponseError.status === 403) {
                  throw new Error('Forbidden error (403): Main page access denied - possibly rate limited');
                }
                if (mainPageResponseError.status === 429) {
                  throw new Error('Rate limit error (429): Too many requests');
                }
              }
              
              // 等待页面加载完成
              await page.waitForTimeout(1000); // 等待页面渲染
              
              // 检查是否被重定向到错误页面（429或403）
              const errorPageCheck = await checkIfErrorPage(page);
              if (errorPageCheck.isErrorPage) {
                if (errorPageCheck.errorCode === '429') {
                  console.warn(`[NAV] Redirected to 429 error page: ${errorPageCheck.url}`);
                  throw new Error(`429 error page detected: redirected to ${errorPageCheck.url}`);
                } else if (errorPageCheck.errorCode === '403') {
                  console.warn(`[NAV] Redirected to 403 error page: ${errorPageCheck.url}`);
                  throw new Error(`403 error page detected: redirected to ${errorPageCheck.url}`);
                }
              }
              
              // 只在页面稳定时检查错误（避免在导航时检查）
              // 注意：API 403 错误不应该阻止页面继续，只有页面显示的错误消息才应该阻止
              try {
                await page.waitForLoadState('domcontentloaded', { timeout: 3000 }).catch(() => {});
                const rateLimitCheck = await checkPageForRateLimitError(page);
                // 只有真正在页面上显示的错误消息才抛出异常
                if (rateLimitCheck.hasError) {
                  console.warn(`[NAV] Rate limit error message detected on page: ${rateLimitCheck.errorText}`);
                  // 不立即抛出，先等待一段时间看是否清除
                  await page.waitForTimeout(3000);
                  const recheck = await checkPageForRateLimitError(page);
                  if (recheck.hasError) {
                    throw new Error(`Rate limit error message on page: ${recheck.errorText}`);
                  }
                }
              } catch (checkError) {
                // 如果是检查时的导航错误，忽略
                if (!checkError.message || (!checkError.message.includes('Execution context') && 
                                            !checkError.message.includes('Rate limit'))) {
                  throw checkError;
                }
              }
              
              return response;
            } finally {
              page.off('response', errorHandler);
            }
          },
          {
            maxRetries: 5, // 增加重试次数
            initialDelay: 8000, // 初始等待时间增加到8秒（403 错误需要更长时间）
            maxDelay: 90000, // 最大等待时间增加到90秒
            backoffMultiplier: 2,
            operationName: `Navigation to ${url}`,
            retryCondition: (error, attempt) => {
              const errorMsg = String(error).toLowerCase();
              // 网络错误、超时、连接失败、速率限制、403、429错误页面 等应该重试
              const shouldRetry = errorMsg.includes('net::') || 
                                 errorMsg.includes('timeout') || 
                                 errorMsg.includes('connection') ||
                                 errorMsg.includes('failed') ||
                                 errorMsg.includes('err_') ||
                                 errorMsg.includes('rate limit') ||
                                 errorMsg.includes('too many requests') ||
                                 errorMsg.includes('429') ||
                                 errorMsg.includes('429 error page') ||
                                 errorMsg.includes('403') ||
                                 errorMsg.includes('forbidden');
              
              if (shouldRetry && (errorMsg.includes('rate limit') || errorMsg.includes('429') || errorMsg.includes('403') || errorMsg.includes('forbidden'))) {
                console.log(`[NAV] Rate limit, 429 error page, or 403 detected, will retry after exponential backoff (attempt ${attempt + 1})`);
              }
              
              return shouldRetry;
            }
          }
        );
        
        resolve(result);
      } catch (error) {
        reject(error);
      }
    }).catch(reject);
  });
}

// 统一的输入框填写函数 - 优化版本，减少重试
async function fillInputFieldOptimized(pageOrFrame, labelPattern, value, options = {}) {
  const {
    inputType = 'input', // 'input', 'textarea', 或 'both'
    verifyAfter = true, // 是否在填写后验证
    timeout = 5000
  } = options;
  
  // 构建选择器
  const inputSelector = inputType === 'both' ? 'input,textarea' : inputType;
  const labelRe = new RegExp(labelPattern, 'i');
  
  // 策略1: 通过 label 的 for 属性定位（最可靠）
  try {
    const labels = await pageOrFrame.locator('label').all();
    for (const label of labels) {
      const labelText = await label.textContent().catch(() => '');
      if (labelRe.test(labelText)) {
        const forId = await label.getAttribute('for').catch(() => null);
        if (forId) {
          const input = pageOrFrame.locator(`#${forId}`);
          if (await input.isVisible({ timeout: 1000 }).catch(() => false)) {
            await input.scrollIntoViewIfNeeded();
            await input.fill('', { force: true });
            await pageOrFrame.waitForTimeout(50);
            await input.fill(value, { force: true });
            if (verifyAfter) {
              const verify = await pageOrFrame.evaluate((id, expected) => {
                const el = document.getElementById(id);
                return el && el.value === expected && el.value.length > 0;
              }, forId, value).catch(() => false);
              if (verify) {
                console.log(`[FILL] Successfully filled via label[for="${forId}"]`);
                return true;
              }
            } else {
              return true;
            }
          }
        }
      }
    }
  } catch (e) {
    // 继续下一个策略
  }
  
  // 策略2: 通过 label 的父容器定位（适用于大多数情况）
  try {
    const label = pageOrFrame.locator('label').filter({ hasText: labelRe }).first();
    if (await label.isVisible({ timeout: 2000 }).catch(() => false)) {
      const container = label.locator('..').locator('..'); // 向上两级
      const input = container.locator(inputSelector).first();
      if (await input.isVisible({ timeout: 2000 }).catch(() => false)) {
        await input.scrollIntoViewIfNeeded();
        const handle = await input.elementHandle();
        if (handle) {
          // 使用 evaluate 直接设置，这是最可靠的方法
          const filled = await pageOrFrame.evaluate((el, val) => {
            el.focus();
            el.value = '';
            el.dispatchEvent(new Event('input', { bubbles: true, cancelable: true }));
            el.value = val;
            el.dispatchEvent(new Event('input', { bubbles: true, cancelable: true }));
            el.dispatchEvent(new Event('change', { bubbles: true, cancelable: true }));
            el.dispatchEvent(new FocusEvent('blur', { bubbles: true }));
            return el.value === val && el.value.length > 0;
          }, handle, value).catch(() => false);
          
          if (filled) {
            console.log(`[FILL] Successfully filled via label container`);
            return true;
          }
          
          // 如果 evaluate 失败，使用 fill 方法
          await input.fill('', { force: true });
          await pageOrFrame.waitForTimeout(50);
          await input.fill(value, { force: true });
          if (verifyAfter) {
            const verify = await pageOrFrame.evaluate((el, expected) => {
              return el.value === expected && el.value.length > 0;
            }, handle, value).catch(() => false);
            if (verify) {
              console.log(`[FILL] Successfully filled via fill() method`);
              return true;
            }
          } else {
            return true;
          }
        }
      }
    }
  } catch (e) {
    // 继续下一个策略
  }
  
  // 策略3: 通过 placeholder 或 aria-label 定位
  try {
    const placeholders = [
      inputType === 'input' ? 'Please enter a public key' : null,
      inputType === 'textarea' ? 'Please enter a signature' : null,
      ...(labelRe.source.includes('public') ? ['public key'] : []),
      ...(labelRe.source.includes('signature') ? ['signature'] : [])
    ].filter(Boolean);
    
    for (const ph of placeholders) {
      const input = pageOrFrame.locator(`${inputSelector}[placeholder*="${ph}" i]`).first();
      if (await input.isVisible({ timeout: 1000 }).catch(() => false)) {
        await input.scrollIntoViewIfNeeded();
        await input.fill('', { force: true });
        await pageOrFrame.waitForTimeout(50);
        await input.fill(value, { force: true });
        if (verifyAfter) {
          const verify = await pageOrFrame.evaluate((el, expected) => {
            return el.value === expected && el.value.length > 0;
          }, await input.elementHandle(), value).catch(() => false);
          if (verify) {
            console.log(`[FILL] Successfully filled via placeholder`);
            return true;
          }
        } else {
          return true;
        }
      }
    }
  } catch (e) {
    // 继续下一个策略
  }
  
  // 策略4: 在页面中直接查找（最后兜底）
  try {
    const found = await pageOrFrame.evaluate((labelPattern, value, inputType) => {
      const labelRe = new RegExp(labelPattern, 'i');
      let target = null;
      
      // 通过 label 查找
      const labels = Array.from(document.querySelectorAll('label'));
      const label = labels.find(l => labelRe.test(l.textContent || ''));
      
      if (label) {
        const forId = label.getAttribute('for');
        if (forId) {
          const el = document.getElementById(forId);
          if (el && (el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement)) {
            target = el;
          }
        }
        
        if (!target) {
          const container = label.closest('div, form, section');
          if (container) {
            const selector = inputType === 'both' ? 'input,textarea' : inputType;
            target = container.querySelector(selector);
          }
        }
      }
      
      // 如果还是没找到，尝试通过 placeholder
      if (!target) {
        const selector = inputType === 'both' ? 'input,textarea' : inputType;
        const inputs = Array.from(document.querySelectorAll(selector));
        target = inputs.find(el => {
          const ph = el.getAttribute('placeholder') || '';
          const aria = el.getAttribute('aria-label') || '';
          return labelRe.test(ph) || labelRe.test(aria);
        });
      }
      
      if (target) {
        target.focus();
        target.value = '';
        target.dispatchEvent(new Event('input', { bubbles: true, cancelable: true }));
        target.value = value;
        target.dispatchEvent(new Event('input', { bubbles: true, cancelable: true }));
        target.dispatchEvent(new Event('change', { bubbles: true, cancelable: true }));
        target.dispatchEvent(new FocusEvent('blur', { bubbles: true }));
        return target.value === value && target.value.length > 0;
      }
      
      return false;
    }, labelPattern, value, inputType).catch(() => false);
    
    if (found) {
      console.log(`[FILL] Successfully filled via evaluate`);
      return true;
    }
  } catch (e) {
    // 所有策略都失败
  }
  
  console.log(`[FILL] Failed to fill input with label pattern: ${labelPattern}`);
  return false;
}

// 签名请求速率限制：全局队列，每次请求间隔至少 500ms
let lastSignRequestTime = 0;
const SIGN_REQUEST_MIN_INTERVAL = 500; // 最小请求间隔（毫秒）

// 检查签名服务是否可用
async function checkSignServiceAvailable(url) {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 3000); // 3秒超时
    
    const response = await fetch(url, {
      method: 'GET',
      signal: controller.signal
    }).catch(() => null);
    
    clearTimeout(timeoutId);
    return response !== null;
  } catch {
    return false;
  }
}

// 带速率限制和重试的签名请求函数
async function signWithRateLimit(addr, hex, retries = 5) {
  const signUrl = `${SIGN_SERVICE_URL}/sign?addr=${encodeURIComponent(addr)}&hex=${encodeURIComponent(hex)}`;
  
  // 首次检查服务是否可用（仅在第一次调用时）
  if (!signWithRateLimit._serviceChecked) {
    console.log(`[SIGN] Checking if signing service is available at ${SIGN_SERVICE_URL}...`);
    const available = await checkSignServiceAvailable(`${SIGN_SERVICE_URL}/sign?addr=test&hex=test`);
    if (!available) {
      console.error(`[SIGN] WARNING: Signing service appears to be unavailable at ${SIGN_SERVICE_URL}`);
      console.error('[SIGN] Will retry anyway, but errors are expected if service is not running.');
    } else {
      console.log(`[SIGN] Signing service is available at ${SIGN_SERVICE_URL}.`);
    }
    signWithRateLimit._serviceChecked = true;
  }
  
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      // 速率限制：确保请求间隔
      const now = Date.now();
      const timeSinceLastRequest = now - lastSignRequestTime;
      if (timeSinceLastRequest < SIGN_REQUEST_MIN_INTERVAL) {
        const waitTime = SIGN_REQUEST_MIN_INTERVAL - timeSinceLastRequest;
        console.log(`[SIGN] Rate limiting: waiting ${waitTime}ms before request...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      }
      
      console.log(`[SIGN] Requesting signature (attempt ${attempt + 1}/${retries}) from ${signUrl.substring(0, 50)}...`);
      
      lastSignRequestTime = Date.now();
      
      // 添加超时控制
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 10000); // 10秒超时
      
      let signResp;
      try {
        signResp = await fetch(signUrl, { signal: controller.signal });
        clearTimeout(timeoutId);
      } catch (fetchError) {
        clearTimeout(timeoutId);
        throw fetchError;
      }
      
      if (!signResp.ok) {
        const errorText = await signResp.text().catch(() => '');
        // 检查是否是速率限制错误
        if (signResp.status === 429 || errorText.toLowerCase().includes('too many requests')) {
          const waitTime = (attempt + 1) * 2000; // 递增等待时间：2s, 4s, 6s
          console.log(`[SIGN] Rate limit detected, waiting ${waitTime}ms before retry...`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
          continue; // 重试
        }
        throw new Error(`Sign service returned ${signResp.status}: ${errorText}`);
      }
      
      const signData = await signResp.json();
      console.log(`[SIGN] Signature received successfully`);
      return signData;
      
    } catch (e) {
      const errorMsg = String(e);
      const errorCause = e.cause ? String(e.cause) : '';
      const errorCode = e.cause?.code || '';
      
      // 构建更详细的错误信息
      let detailedError = errorMsg;
      if (errorCode) {
        detailedError += ` (code: ${errorCode})`;
      }
      if (errorCause && errorCause !== errorMsg) {
        detailedError += ` - ${errorCause}`;
      }
      
      console.error(`[SIGN] Error (attempt ${attempt + 1}/${retries}): ${detailedError}`);
      
      // 检查是否是连接错误（服务不可用）
      const isConnectionError = errorCode === 'ECONNREFUSED' || 
                                errorCode === 'ETIMEDOUT' ||
                                errorCode === 'ENOTFOUND' ||
                                errorMsg.toLowerCase().includes('fetch failed') ||
                                errorMsg.toLowerCase().includes('connection refused') ||
                                errorMsg.toLowerCase().includes('econnrefused');
      
      // 检查是否是速率限制相关的错误
      const isRateLimitError = errorMsg.toLowerCase().includes('too many requests') || 
                               errorMsg.toLowerCase().includes('rate limit');
      
      // 最后一次尝试失败，抛出更详细的错误
      if (attempt === retries - 1) {
        if (isConnectionError) {
          throw new Error(`Failed to connect to signing service at ${SIGN_SERVICE_URL} after ${retries} attempts. Please ensure the service is running. Original error: ${detailedError}`);
        }
        throw new Error(`Sign request failed after ${retries} attempts: ${detailedError}`);
      }
      
      // 计算等待时间（连接错误等待更长时间）
      let waitTime;
      if (isConnectionError) {
        // 连接错误：指数退避，最大30秒
        waitTime = Math.min(2000 * Math.pow(2, attempt), 30000);
        console.log(`[SIGN] Connection error detected, waiting ${waitTime}ms before retry (service may be starting up)...`);
      } else if (isRateLimitError) {
        waitTime = (attempt + 1) * 2000;
        console.log(`[SIGN] Rate limit error detected, waiting ${waitTime}ms before retry...`);
      } else {
        // 其他错误：递增等待时间
        waitTime = 1000 * (attempt + 1);
        console.log(`[SIGN] Retrying in ${waitTime}ms...`);
      }
      
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
  }
  throw new Error('Sign request failed after all retries');
}

// 从 JSON 文件或文件夹加载任务列表
// ⚠️ 默认从项目根目录下的 task 文件夹读取任务列表
// 如果路径指向文件夹，会读取文件夹中所有 .json 文件并合并
// 如果路径指向文件，则直接读取该文件
const TASKS_PATH = process.env.TASKS_FILE || join(__dirname, '..', 'task');

function loadTasks() {
  try {
    const stats = statSync(TASKS_PATH);
    let allTasks = [];
    let loadedFiles = [];
    
    if (stats.isDirectory()) {
      // 如果是文件夹，读取文件夹中所有 .json 文件
      console.log(`[CONFIG] Loading tasks from directory: ${TASKS_PATH}`);
      const files = readdirSync(TASKS_PATH).filter(f => extname(f).toLowerCase() === '.json');
      
      if (files.length === 0) {
        throw new Error(`No JSON files found in directory: ${TASKS_PATH}`);
      }
      
      for (const file of files) {
        const filePath = join(TASKS_PATH, file);
        try {
          const fileContent = readFileSync(filePath, 'utf8');
          const data = JSON.parse(fileContent);
          // 支持两种格式：直接是数组，或者包装在 tasks 属性中
          const tasks = Array.isArray(data) ? data : (data.tasks || []);
          
          if (Array.isArray(tasks) && tasks.length > 0) {
            allTasks = allTasks.concat(tasks);
            loadedFiles.push(`${file} (${tasks.length} tasks)`);
            console.log(`[CONFIG]   ✓ Loaded ${tasks.length} task(s) from ${file}`);
          }
        } catch (err) {
          console.warn(`[CONFIG]   ⚠️ Failed to load ${file}: ${err.message}`);
        }
      }
      
      if (allTasks.length === 0) {
        throw new Error(`No valid tasks found in any JSON file in directory: ${TASKS_PATH}`);
      }
      
      console.log(`[CONFIG] Loaded ${allTasks.length} task(s) from ${loadedFiles.length} file(s)`);
    } else if (stats.isFile()) {
      // 如果是文件，直接读取
      console.log(`[CONFIG] Loading tasks from file: ${TASKS_PATH}`);
      const fileContent = readFileSync(TASKS_PATH, 'utf8');
      const data = JSON.parse(fileContent);
      // 支持两种格式：直接是数组，或者包装在 tasks 属性中
      allTasks = Array.isArray(data) ? data : (data.tasks || []);
      
      if (!Array.isArray(allTasks) || allTasks.length === 0) {
        throw new Error('Tasks file must contain a non-empty array of tasks');
      }
      
      console.log(`[CONFIG] Loaded ${allTasks.length} task(s) from ${TASKS_PATH}`);
    } else {
      throw new Error(`Path is neither a file nor a directory: ${TASKS_PATH}`);
    }
    
    // 验证每个任务都有必需的字段
    for (let i = 0; i < allTasks.length; i++) {
      const task = allTasks[i];
      if (!task.id) {
        throw new Error(`Task at index ${i} is missing required field 'id'`);
      }
      if (!task.addr) {
        throw new Error(`Task at index ${i} (id: ${task.id}) is missing required field 'addr'`);
      }
    }
    
    return allTasks;
  } catch (error) {
    if (error.code === 'ENOENT') {
      console.error(`[ERROR] Tasks path not found: ${TASKS_PATH}`);
      console.error(`[ERROR] Please create a task directory or file, or set TASKS_FILE environment variable`);
      console.error(`[ERROR] Example: TASKS_FILE=./my-tasks.json node runbatch.mjs`);
      console.error(`[ERROR] Example: TASKS_FILE=./task node runbatch.mjs`);
    } else {
      console.error(`[ERROR] Failed to load tasks from ${TASKS_PATH}:`, error.message);
    }
    process.exit(1);
  }
}

const tasks = loadTasks();

// 初始化任务（只完成流程到挖矿页面，不启动挖矿）
async function runOneInitOnly(task, scheduler = null) {
  // 复用runOne的逻辑，但在建立session后不启动，而是注册到scheduler
  // 这个函数会完成整个流程直到建立session，但不会点击Start
  return await runOne(task, { initOnly: true, scheduler });
}

async function runOne(task, options = {}) {
  const { initOnly = false, scheduler = null } = options;
  
  // ⚠️ 记录任务开始时间（页面打开时间），任务进入"登录阶段"
  const taskId = task.id;
  if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
    if (!taskStats.taskTimers.has(taskId)) {
      taskStats.taskTimers.set(taskId, { pageOpenTime: Date.now() });
    } else {
      taskStats.taskTimers.get(taskId).pageOpenTime = Date.now();
    }
    taskStats.loggingIn++;
    console.log(`[STATS] 🔐 Task ${taskId} started (logging in, Logging In: ${taskStats.loggingIn})`);
  }
  
  // 支持 headless 模式（可通过环境变量控制）
  const HEADLESS = process.env.HEADLESS !== 'false'; // 默认 headless 模式
  const DISPLAY = process.env.DISPLAY || ':99';
  
  const browser = await chromium.launch({
    headless: HEADLESS, // 可通过环境变量 HEADLESS=false 启用可视化
    args: [
      '--guest',
      '--no-first-run',
      '--no-default-browser-check',
      '--disable-dev-shm-usage',
      ...(HEADLESS ? [
        '--headless=new',
        '--disable-web-security',
        '--disable-features=IsolateOrigins,site-per-process',
        '--disable-site-isolation-trials',
        '--disable-blink-features=AutomationControlled',
        '--disable-setuid-sandbox',
        '--no-sandbox',
        '--disable-extensions',
        '--disable-plugins',
        '--disable-background-networking',
        '--disable-background-timer-throttling',
        '--disable-renderer-backgrounding',
        '--disable-backgrounding-occluded-windows',
        '--disable-ipc-flooding-protection',
        // 添加更多反检测参数
        '--lang=en-US,en',
        '--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      ] : [
        `--display=${DISPLAY}`,
        '--disable-gpu',
      ])
    ]
  });
  
  // 创建 context，添加真实浏览器的特征（特别是 headless 模式下）
  const context = await browser.newContext({
    viewport: { width: 1920, height: 1080 },
    userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    locale: 'en-US',
    timezoneId: 'America/New_York',
    permissions: [],
    // 添加真实的浏览器特征（移除 Cache-Control 以避免 CORS 问题）
    extraHTTPHeaders: {
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
      'Accept-Encoding': 'gzip, deflate, br',
      'Connection': 'keep-alive',
      'Upgrade-Insecure-Requests': '1',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'none',
      'Sec-Fetch-User': '?1',
      // 移除 Cache-Control，因为会导致 Google Fonts 的 CORS 错误
      // 'Cache-Control': 'max-age=0',
    },
  });
  
  const page = await context.newPage();
  
  // 在 headless 模式下，注入脚本隐藏 webdriver 特征
  if (HEADLESS) {
    await page.addInitScript(() => {
      // 隐藏 webdriver 特征
      Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
      });
      
      // 覆盖 chrome 对象
      window.chrome = {
        runtime: {},
      };
      
      // 添加 plugins
      Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5],
      });
      
      // 添加 languages
      Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
      });
      
      // 覆盖 permissions
      const originalQuery = window.navigator.permissions.query;
      window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
          Promise.resolve({ state: Notification.permission }) :
          originalQuery(parameters)
      );
    });
  }
  // 打印浏览器控制台日志与错误（过滤掉非关键错误以减少日志噪音）
  page.on('console', (msg) => {
    try { 
      const text = msg.text();
      // 过滤掉以下非关键错误（不影响功能）：
      // - 字体相关的 CORS 错误
      // - 资源加载失败（ERR_FAILED, ERR_ABORTED）
      // - 非关键资源的加载错误
      const shouldIgnore = 
        text.includes('CORS policy') ||
        text.includes('font') ||
        text.includes('Access to font') ||
        text.includes('Failed to load resource') ||
        text.includes('net::ERR_FAILED') ||
        text.includes('net::ERR_ABORTED') ||
        /fonts\.gstatic\.com/.test(text) ||
        /\.woff2?/.test(text);
      
      if (!shouldIgnore) {
        console.log('[PAGE]', msg.type(), text); 
      }
    } catch {}
  });
  page.on('pageerror', (err) => {
    try { 
      const errMsg = String(err);
      // 过滤掉非关键错误
      const shouldIgnore = 
        errMsg.includes('CORS') ||
        errMsg.includes('font') ||
        errMsg.includes('Failed to load resource') ||
        errMsg.includes('net::ERR_FAILED') ||
        errMsg.includes('net::ERR_ABORTED');
      
      if (!shouldIgnore) {
        console.log('[PAGE][error]', errMsg); 
      }
    } catch {}
  });
  
  // 过滤掉非关键资源加载失败的错误（减少日志噪音）
  page.on('requestfailed', (request) => {
    const url = request.url();
    const failure = request.failure();
    
    // 静默忽略以下资源的加载失败（不影响功能）：
    // 1. 字体文件
    // 2. 图片资源（.jpg, .png, .gif, .svg, .webp）
    // 3. 第三方统计/广告服务
    // 4. 其他非关键资源
    const shouldIgnore = 
      url.includes('fonts.gstatic.com') ||
      url.includes('.woff2') ||
      url.includes('.woff') ||
      url.includes('.ttf') ||
      url.includes('.eot') ||
      /\.(jpg|jpeg|png|gif|svg|webp|ico)$/i.test(url) ||
      url.includes('google-analytics') ||
      url.includes('googletagmanager') ||
      url.includes('doubleclick') ||
      url.includes('googleadservices') ||
      url.includes('analytics') ||
      failure?.errorText === 'net::ERR_FAILED' || // 一般性的网络失败
      failure?.errorText === 'net::ERR_ABORTED';  // 被中止的请求
    
    if (shouldIgnore) {
      return; // 不输出日志
    }
    
    // 只记录关键资源的失败（如 API 调用失败）
    // console.warn(`[PAGE] Request failed: ${url} - ${failure?.errorText || 'unknown'}`);
  });
  
  // 监听 API 响应错误（特别是 403），但不阻止流程
  // API 403 错误可能是后台调用失败，不影响页面功能
  let api403Count = 0;
  page.on('response', (response) => {
    if (response.status() === 403) {
      api403Count++;
      // 只在第一次或每10次时记录，避免日志过多
      if (api403Count === 1 || api403Count % 10 === 0) {
        console.warn(`[API-403] 403 Forbidden detected on: ${response.url()} (count: ${api403Count}, this may be normal for API calls)`);
      }
    } else if (response.status() === 429) {
      console.warn(`[API-429] Rate limit detected on: ${response.url()}`);
    }
  });

  // 后台监控：定期检查速率限制错误和 429 错误页面，防止任务卡死
  let rateLimitMonitorInterval = null;
  let rateLimitErrorDetected = false;
  
  const startRateLimitMonitor = () => {
    if (rateLimitMonitorInterval) return; // 已经启动
    
    rateLimitMonitorInterval = setInterval(async () => {
      try {
        // 只在检测到 429 错误页面时才处理（其他错误页面在主流程中处理）
        const errorPageCheck = await checkIfErrorPage(page);
        if (errorPageCheck.isErrorPage && errorPageCheck.errorCode === '429') {
          console.warn(`[429-MONITOR] Background monitor detected 429 error page: ${errorPageCheck.url}`);
          console.warn(`[429-MONITOR] Attempting automatic recovery via disconnect...`);
          // 尝试自动恢复（但不阻塞主流程）
          handleErrorPage(page, BASE_URL).catch(err => {
            console.warn(`[429-MONITOR] Auto-recovery failed: ${err.message}`);
          });
          return;
        }
        
        // 检查速率限制错误
        const check = await checkPageForRateLimitError(page, 1);
        if (check.hasError && !rateLimitErrorDetected) {
          rateLimitErrorDetected = true;
          console.warn(`[RATE-LIMIT-MONITOR] Background monitor detected rate limit error: ${check.errorText}`);
          console.warn(`[RATE-LIMIT-MONITOR] Will wait for error to clear...`);
        } else if (!check.hasError && rateLimitErrorDetected) {
          rateLimitErrorDetected = false;
          console.log(`[RATE-LIMIT-MONITOR] Rate limit error cleared!`);
        }
      } catch (e) {
        // 忽略监控错误，不影响主流程
      }
    }, 5000); // 每5秒检查一次（减少频率）
  };
  
  const stopRateLimitMonitor = () => {
    if (rateLimitMonitorInterval) {
      clearInterval(rateLimitMonitorInterval);
      rateLimitMonitorInterval = null;
    }
  };

  try {
    page.setDefaultTimeout(20_000);

    // 启动后台监控
    startRateLimitMonitor();

    // 使用速率限制的页面导航（带重试）
    await gotoWithRateLimit(page, BASE_URL, { waitUntil: 'domcontentloaded' });
    
    // 导航后检查页面错误（只检查页面显示的错误，不检查API错误）
    // 等待页面加载稳定后再检查
    await page.waitForTimeout(2000);
    
    // 首先检查是否被重定向到错误页面（429或403需要 disconnect）
    const errorPageCheck = await checkIfErrorPage(page);
    if (errorPageCheck.isErrorPage && (errorPageCheck.errorCode === '429' || errorPageCheck.errorCode === '403')) {
      const errorType = errorPageCheck.errorCode;
      console.warn(`[${errorType}-ERROR] Detected ${errorType} error page after navigation: ${errorPageCheck.url}`);
      const handled = await handleErrorPage(page, BASE_URL);
      if (handled) {
        await page.waitForTimeout(2000);
        // 如果成功重置，可能需要从头开始，但先检查页面状态
        const isReset = await page.getByText('Enter an address manually', { exact: true }).isVisible({ timeout: 3000 }).catch(() => false);
        if (isReset) {
          console.log(`[${errorType}-ERROR] Page reset successful, will continue from beginning...`);
        }
      }
    } else if (errorPageCheck.isErrorPage && errorPageCheck.errorCode === 'other') {
      // 其他错误页面：直接导航回去，不使用 disconnect
      console.warn(`[ERROR-PAGE] Detected non-429 error page after navigation: ${errorPageCheck.url}, navigating back...`);
      try {
        await page.goto(BASE_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
        await page.waitForTimeout(2000);
      } catch (e) {
        console.warn(`[ERROR-PAGE] Failed to navigate back: ${e.message}`);
      }
    }
    
    const postNavCheck = await checkPageForRateLimitError(page);
    if (postNavCheck.hasError) {
      console.warn(`[RATE-LIMIT] Rate limit error message detected after navigation: ${postNavCheck.errorText}`);
      console.log(`[RATE-LIMIT] Waiting for error message to clear (max 30s)...`);
      const cleared = await waitForRateLimitErrorToClear(page, 30000, 3000, BASE_URL);
      if (!cleared) {
        // 如果等待后仍有错误，检查是否在 429 错误页面（只有 429 才需要 disconnect）
        const finalErrorPageCheck = await checkIfErrorPage(page);
        if (finalErrorPageCheck.isErrorPage && finalErrorPageCheck.errorCode === '429') {
          console.log(`[429-ERROR] Still on 429 error page after waiting, attempting recovery via disconnect...`);
          const handled = await handleErrorPage(page, BASE_URL);
          if (handled) {
            await page.waitForTimeout(2000);
            // 检查是否重置成功，如果成功可能需要从头开始
            const isReset = await page.getByText('Enter an address manually', { exact: true }).isVisible({ timeout: 3000 }).catch(() => false);
            if (isReset) {
              console.log('[429-ERROR] Page reset successful, will retry from beginning...');
              // 重置成功，抛出特殊错误让外部重试机制从头开始
              throw new Error('429 error page reset successful, retry from beginning');
            }
          }
        } else if (finalErrorPageCheck.isErrorPage && finalErrorPageCheck.errorCode === 'other') {
          // 其他错误页面：直接导航回去
          console.warn(`[ERROR-PAGE] Still on non-429 error page (${finalErrorPageCheck.url}), navigating back...`);
          try {
            await page.goto(BASE_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
            await page.waitForTimeout(2000);
          } catch (e) {
            console.warn(`[ERROR-PAGE] Failed to navigate back: ${e.message}`);
          }
        } else {
          // 尝试刷新页面一次
          console.log(`[RATE-LIMIT] Error message persists, refreshing page once...`);
          try {
            await page.reload({ waitUntil: 'domcontentloaded', timeout: 15000 });
            await page.waitForTimeout(2000);
            
            // 刷新后检查是否跳转到错误页面
            const afterRefreshErrorCheck = await checkIfErrorPage(page);
            if (afterRefreshErrorCheck.isErrorPage && afterRefreshErrorCheck.errorCode === '429') {
              await handleErrorPage(page, BASE_URL);
              await page.waitForTimeout(2000);
            }
            
            const afterReloadCheck = await checkPageForRateLimitError(page);
            if (afterReloadCheck.hasError) {
              // 即使刷新后还有错误，也继续执行，因为可能是暂时的或API错误
              console.warn(`[RATE-LIMIT] Error message persists after refresh, but continuing anyway...`);
            }
          } catch (reloadError) {
            console.warn(`[RATE-LIMIT] Failed to refresh: ${reloadError.message}, continuing...`);
          }
        }
      }
    }

    // ⚠️ 在点击按钮前，先检查是否已经在 wallet 页面（可能由于之前的状态）
    const urlBeforeClick = page.url();
    if (urlBeforeClick.includes('/wizard/wallet')) {
      console.log(`[WALLET-PAGE] Page is already at wallet page (${urlBeforeClick}) before clicking button, will handle in retry function`);
    }

    // 点击 Enter an address manually（带重试）
    await retryWithBackoff(
      async () => {
        // ⚠️ 在执行操作前检查当前页面URL，如果在 wallet 页面需要特殊处理
        const currentUrlCheck = page.url();
        if (currentUrlCheck.includes('/wizard/wallet')) {
          console.log(`[WALLET-PAGE] Detected wallet page at start of retry (${currentUrlCheck})`);
        }
        
        // 在执行操作前检查是否在 429 错误页面（只有 429 才需要 disconnect）
        const preErrorCheck = await checkIfErrorPage(page);
        if (preErrorCheck.isErrorPage && preErrorCheck.errorCode === '429') {
          console.warn(`[429-ERROR] Detected 429 error page before clicking button, attempting recovery via disconnect...`);
          const handled = await handleErrorPage(page, BASE_URL);
          if (!handled) {
            throw new Error(`429 error page detected and recovery failed: ${preErrorCheck.url}`);
          }
          await page.waitForTimeout(2000);
          // 恢复成功后，确保页面已重置
          const isReset = await page.getByText('Enter an address manually', { exact: true }).isVisible({ timeout: 5000 }).catch(() => false);
          if (!isReset) {
            throw new Error('429 error page recovery attempted but not in initial state');
          }
          // 页面已重置到初始状态，可以继续执行（从头开始流程）
          console.log('[429-ERROR] Page reset successful, continuing from initial state...');
          // 不需要抛出错误，直接继续执行后续步骤
        } else if (preErrorCheck.isErrorPage && preErrorCheck.errorCode === 'other') {
          // 其他错误页面：直接导航回去
          console.warn(`[ERROR-PAGE] Detected non-429 error page before clicking button, navigating back...`);
          try {
            await page.goto(BASE_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
            await page.waitForTimeout(2000);
          } catch (e) {
            console.warn(`[ERROR-PAGE] Failed to navigate back: ${e.message}`);
          }
        }
        
        // 在执行操作前检查是否有速率限制错误
        const preCheck = await checkPageForRateLimitError(page);
        if (preCheck.hasError) {
          throw new Error(`Rate limit error detected before clicking button: ${preCheck.errorText}`);
        }
        
        // 等待页面稳定后再操作（避免用户交互干扰）
        await waitForPageStable(page, 2000);
        
        // ⚠️ 用户要求：无论URL是什么，如果页面出现"Choose a Destination address"就填写地址继续下一步
        // 先检查页面内容，不依赖URL
        const pageContent = await page.evaluate(() => {
          return (document.body?.innerText || '').toLowerCase();
        }).catch(() => '');
        
        const hasChooseDestination = /choose.*destination.*address/i.test(pageContent);
        const hasEnterAddressManually = /enter.*address.*manually/i.test(pageContent);
        
        // ⚠️ 检查地址输入框是否已经可见（避免重复点击按钮）
        let addressInputVisible = false;
        try {
          const inputCheck1 = page.getByPlaceholder('Enter address');
          addressInputVisible = await inputCheck1.first().isVisible({ timeout: 1000 }).catch(() => false);
          if (!addressInputVisible) {
            const textboxes = page.locator('input[type="text"], input:not([type]), textarea');
            const count = await textboxes.count();
            for (let i = 0; i < Math.min(count, 5); i++) {
              const tb = textboxes.nth(i);
              if (await tb.isVisible({ timeout: 500 }).catch(() => false)) {
                const placeholder = await tb.getAttribute('placeholder').catch(() => '');
                const ariaLabel = await tb.getAttribute('aria-label').catch(() => '');
                if (placeholder && /address/i.test(placeholder) || ariaLabel && /address/i.test(ariaLabel)) {
                  addressInputVisible = true;
                  break;
                }
              }
            }
          }
        } catch (e) {
          // 忽略错误，继续检查
        }
        
        // ⚠️ 如果页面显示"Choose a Destination address"，无论输入框是否可见，都必须点击"Enter an address manually"按钮
        // ⚠️ 用户反馈：页面在 /wizard/wallet 或 /wizard/mine 卡在了"Choose a Destination address"子页面，没有点击"Enter an address"
        const currentUrl = page.url();
        const isWalletPage = currentUrl.includes('/wizard/wallet');
        const isMinePage = currentUrl.includes('/wizard/mine');
        const needsButtonClick = hasChooseDestination || isWalletPage || isMinePage || hasEnterAddressManually;
        
        // ⚠️ 只有当地址输入框真正可见且页面没有"Choose a Destination address"时，才跳过点击按钮的步骤
        if (addressInputVisible && !hasChooseDestination) {
          console.log(`[WALLET-PAGE] ✓ Address input already visible and no "Choose a Destination address" detected, skipping button click`);
          // 跳过按钮点击，直接进入填写地址的步骤
        } else {
          // ⚠️ 如果页面显示"Choose a Destination address"，或者输入框不可见，或者是在 wallet/mine 页面，都需要点击按钮
          if (hasChooseDestination) {
            console.log(`[WALLET-PAGE] ⚠️ Page shows "Choose a Destination address", will click "Enter an address manually" button to proceed`);
          } else if (!addressInputVisible) {
            console.log(`[WALLET-PAGE] ⚠️ Address input not visible, will click button to show input`);
          } else if (isWalletPage || isMinePage) {
            console.log(`[WALLET-PAGE] ⚠️ On ${isWalletPage ? 'wallet' : 'mine'} page, will click button to ensure input is shown`);
          }
          
          if (needsButtonClick) {
            console.log(`[WALLET-PAGE] Detected wallet page or "Enter an address manually" text (${currentUrl}), checking for button...`);
            
            // 尝试多种方式找到按钮
            let btn = null;
            let btnFound = false;
            
            // 方法1: 精确文本匹配
            try {
              btn = page.getByText('Enter an address manually', { exact: true });
              btnFound = await btn.isVisible({ timeout: 3000 }).catch(() => false);
              if (btnFound) {
                console.log(`[WALLET-PAGE] Found "Enter an address manually" button (exact match)`);
              }
            } catch (e) {
              // 继续尝试其他方法
            }
            
            // 方法2: 不区分大小写匹配
            if (!btnFound) {
              try {
                btn = page.getByText(/enter.*address.*manually/i);
                btnFound = await btn.first().isVisible({ timeout: 3000 }).catch(() => false);
                if (btnFound) {
                  console.log(`[WALLET-PAGE] Found "Enter an address manually" button (case-insensitive)`);
                  btn = btn.first();
                }
              } catch (e) {
                // 继续尝试其他方法
              }
            }
            
            // 方法3: 查找包含 "manually" 的按钮
            if (!btnFound) {
              try {
                const buttons = page.locator('button').filter({ hasText: /manually/i });
                const count = await buttons.count();
                if (count > 0) {
                  btn = buttons.first();
                  btnFound = await btn.isVisible({ timeout: 3000 }).catch(() => false);
                  if (btnFound) {
                    console.log(`[WALLET-PAGE] Found button with "manually" text`);
                  }
                }
              } catch (e) {
                // 继续尝试其他方法
              }
            }
            
            // 方法4: 查找所有可见按钮，找到包含 "address" 和 "enter" 的
            if (!btnFound) {
              try {
                const allButtons = page.locator('button');
                const count = await allButtons.count();
                for (let i = 0; i < Math.min(count, 50); i++) {
                  const button = allButtons.nth(i);
                  const isVisible = await button.isVisible({ timeout: 500 }).catch(() => false);
                  if (isVisible) {
                    const text = await button.textContent().catch(() => '');
                    if (text && /enter.*address.*manually/i.test(text.trim())) {
                      btn = button;
                      btnFound = true;
                      console.log(`[WALLET-PAGE] Found button by scanning: "${text.trim()}"`);
                      break;
                    }
                  }
                }
              } catch (e) {
                // 继续尝试默认方法
              }
            }
            
            // 方法5: 使用 CSS 选择器直接查找（基于用户提供的 HTML 结构）
            if (!btnFound) {
              try {
                // 查找包含 "Enter an address manually" 文本的按钮
                const btnBySelector = page.locator('button:has-text("Enter an address manually")');
                const count = await btnBySelector.count();
                if (count > 0) {
                  for (let i = 0; i < count; i++) {
                    const button = btnBySelector.nth(i);
                    const isVisible = await button.isVisible({ timeout: 1000 }).catch(() => false);
                    if (isVisible) {
                      const text = await button.textContent().catch(() => '');
                      if (text && text.trim() === 'Enter an address manually') {
                        btn = button;
                        btnFound = true;
                        console.log(`[WALLET-PAGE] Found button by CSS selector: "${text.trim()}"`);
                        break;
                      }
                    }
                  }
                }
              } catch (e) {
                console.warn(`[WALLET-PAGE] Method 5 (CSS selector) failed: ${e.message}`);
              }
            }
            
            // 方法6: 使用 page.evaluate 直接在页面中查找并点击按钮
            if (!btnFound) {
              try {
                const clicked = await page.evaluate(() => {
                  const buttons = Array.from(document.querySelectorAll('button'));
                  for (const btn of buttons) {
                    const text = (btn.textContent || '').trim();
                    if (text.toLowerCase() === 'enter an address manually' || 
                        /^enter\s+an\s+address\s+manually$/i.test(text)) {
                      // 检查按钮是否可见且可点击
                      const rect = btn.getBoundingClientRect();
                      const style = window.getComputedStyle(btn);
                      const isVisible = rect.width > 0 && rect.height > 0 && 
                                       style.visibility !== 'hidden' &&
                                       style.display !== 'none' &&
                                       !btn.disabled;
                      if (isVisible) {
                        // 滚动到按钮位置
                        btn.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        // 直接点击按钮
                        btn.click();
                        return { clicked: true, text: text };
                      }
                    }
                  }
                  return { clicked: false };
                }).catch(() => ({ clicked: false }));
                
                if (clicked.clicked) {
                  btnFound = true;
                  // 标记为已点击，避免后续重复点击
                  btn = { clicked: true };
                  console.log(`[WALLET-PAGE] Found and clicked button by page.evaluate: "${clicked.text}"`);
                  // 等待页面响应
                  await page.waitForTimeout(1000);
                }
              } catch (e) {
                console.warn(`[WALLET-PAGE] Method 6 (page.evaluate) failed: ${e.message}`);
              }
            }
            
            if (btnFound && btn && !btn.clicked) {
              // 只有当btn是Playwright locator且未被点击时，才执行点击
              console.log(`[WALLET-PAGE] Clicking "Enter an address manually" button on wallet page...`);
              await safeClick(page, btn, { timeout: 10000, force: true, retries: 3 });
            } else if (btnFound && btn && btn.clicked) {
              // 方法6已经点击了按钮，跳过点击步骤
              console.log(`[WALLET-PAGE] Button already clicked via page.evaluate, continuing...`);
            }
            
            // ⚠️ 无论用哪种方法点击了按钮，都需要等待输入框出现并继续流程
            if (btnFound) {
              // ⚠️ 用户要求：点击Enter an address manually后，应该等待输入框出现并继续流程
              // 等待输入框出现（最多等待10秒）
              let inputVisible = false;
              for (let waitAttempt = 0; waitAttempt < 10; waitAttempt++) {
                await page.waitForTimeout(500);
                try {
                  // 尝试多种方式查找输入框
                  const inputByPlaceholder = page.getByPlaceholder(/enter.*address/i);
                  inputVisible = await inputByPlaceholder.first().isVisible({ timeout: 1000 }).catch(() => false);
                  if (!inputVisible) {
                    // 尝试通过input type查找
                    const inputByType = page.locator('input[type="text"]');
                    const count = await inputByType.count();
                    for (let i = 0; i < count; i++) {
                      const input = inputByType.nth(i);
                      const isVisible = await input.isVisible({ timeout: 500 }).catch(() => false);
                      if (isVisible) {
                        const placeholder = await input.getAttribute('placeholder').catch(() => '');
                        if (placeholder && /address/i.test(placeholder)) {
                          inputVisible = true;
                          break;
                        }
                      }
                    }
                  }
                  if (inputVisible) {
                    console.log(`[WALLET-PAGE] ✓ Address input is now visible after clicking button`);
                    break;
                  }
                } catch (e) {
                  // 继续等待
                }
              }
              
              if (!inputVisible) {
                console.warn(`[WALLET-PAGE] ⚠️ Button clicked but address input not visible after waiting, will try to fill anyway...`);
                // 即使输入框不可见，也尝试填写（可能已经出现但检测不到）
                await page.waitForTimeout(1000);
              }
            } else {
              console.warn(`[WALLET-PAGE] "Enter an address manually" button not found on wallet/mine page, trying default method...`);
              // 如果找不到，尝试默认方法
              try {
                const defaultBtn = page.getByText('Enter an address manually', { exact: true });
                await safeClick(page, defaultBtn, { timeout: 10000, force: true, retries: 3 });
                // 等待输入框出现
                await page.waitForTimeout(2000);
                console.log(`[WALLET-PAGE] ✓ Successfully clicked "Enter an address manually" button using default method`);
              } catch (e) {
                console.warn(`[WALLET-PAGE] ⚠️ Failed to click button using default method: ${e.message}, but continuing...`);
                // 即使失败也继续，可能输入框已经可见
              }
            }
          } else {
            // 不在 wallet/mine 页面，但仍需要点击按钮（可能因为其他原因进入此分支）
            console.log(`[WALLET-PAGE] Attempting to find and click "Enter an address manually" button using default method...`);
            try {
              const btn = page.getByText('Enter an address manually', { exact: true });
              // 使用安全点击，确保即使有用户交互也能成功
              await safeClick(page, btn, { timeout: 10000, force: true, retries: 3 });
              // 等待输入框出现
              await page.waitForTimeout(2000);
              console.log(`[WALLET-PAGE] ✓ Successfully clicked "Enter an address manually" button using default method`);
            } catch (e) {
              console.warn(`[WALLET-PAGE] ⚠️ Failed to click button using default method: ${e.message}, but continuing...`);
              // 即使失败也继续，可能输入框已经可见
            }
          }
        }
        
        // 点击后等待并检查错误
        await page.waitForTimeout(1000);
        
        // 检查是否跳转到 429 错误页面（只有 429 才需要 disconnect）
        const postErrorCheck = await checkIfErrorPage(page);
        if (postErrorCheck.isErrorPage && postErrorCheck.errorCode === '429') {
          console.warn(`[429-ERROR] Detected 429 error page after clicking button, attempting recovery via disconnect...`);
          const handled = await handleErrorPage(page, BASE_URL);
          if (!handled) {
            throw new Error(`429 error page detected after click and recovery failed: ${postErrorCheck.url}`);
          }
          await page.waitForTimeout(2000);
          // 恢复成功后，检查页面是否重置
          const isReset = await page.getByText('Enter an address manually', { exact: true }).isVisible({ timeout: 5000 }).catch(() => false);
          if (isReset) {
            console.log('[429-ERROR] Page reset successful after click, continuing from initial state...');
            // 页面已重置，继续执行（会重新点击 "Enter an address manually"）
            // 但此时按钮已存在，所以可以直接继续
          } else {
            throw new Error('429 error page detected after click, recovery attempted but page not in initial state');
          }
        } else if (postErrorCheck.isErrorPage && postErrorCheck.errorCode === 'other') {
          // 其他错误页面：直接导航回去
          console.warn(`[ERROR-PAGE] Detected non-429 error page after clicking button, navigating back...`);
          try {
            await page.goto(BASE_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
            await page.waitForTimeout(2000);
          } catch (e) {
            console.warn(`[ERROR-PAGE] Failed to navigate back: ${e.message}`);
          }
        }
        
        const postCheck = await checkPageForRateLimitError(page);
        if (postCheck.hasError) {
          throw new Error(`Rate limit error detected after clicking button: ${postCheck.errorText}`);
        }
      },
      {
        maxRetries: 3, // 减少重试次数，避免过度等待
        initialDelay: 3000, // 减少初始延迟
        maxDelay: 30000, // 减少最大延迟
        backoffMultiplier: 2,
        operationName: 'Click "Enter an address manually" button',
        retryCondition: (error, attempt) => {
          const errorMsg = String(error).toLowerCase();
          // 只对真正的错误消息重试，不重试API错误
          return (errorMsg.includes('rate limit') && errorMsg.includes('message')) || 
                 errorMsg.includes('too many requests') ||
                 errorMsg.includes('wait.*try again') ||
                 errorMsg.includes('timeout') ||
                 errorMsg.includes('not found');
        }
      }
    );

    // 等待输入框出现并稳健填入地址（多策略兜底）
    const fillAddress = async (addr) => {
      // 策略1：placeholder 精确
      const loc1 = page.getByPlaceholder('Enter address');
      if (await loc1.first().isVisible().catch(() => false)) {
        await loc1.first().fill(addr);
        return true;
      }
      // 策略2：role=textbox 且可见
      const loc2 = page.getByRole('textbox', { name: /address/i });
      if (await loc2.first().isVisible().catch(() => false)) {
        await loc2.first().fill(addr);
        return true;
      }
      // 策略3：页面上唯一/第一个可编辑文本框
      const textboxes = page.locator('input[type="text"], input:not([type]), textarea');
      await textboxes.first().waitFor({ state: 'visible', timeout: 10000 }).catch(() => {});
      if (await textboxes.first().isVisible().catch(() => false)) {
        await textboxes.first().fill(addr);
        return true;
      }
      return false;
    };

    const filled = await fillAddress(task.addr);
    if (!filled) {
      throw new Error('Address input not found/filled');
    }
    // 其它参数...
    // await page.fill('input[name="extra"]', task.extra);

    // 点击 Continue 前检查 429 错误页面（只有 429 才需要 disconnect）
    const continuePreErrorCheck = await checkIfErrorPage(page);
    if (continuePreErrorCheck.isErrorPage && continuePreErrorCheck.errorCode === '429') {
      console.warn(`[429-ERROR] Detected 429 error page before Continue, attempting recovery via disconnect...`);
      await handleErrorPage(page, BASE_URL);
      await page.waitForTimeout(2000);
    }
    
    // 点击 Continue 前检查错误
    const continueCheck = await checkPageForRateLimitError(page);
    if (continueCheck.hasError) {
      console.warn(`[RATE-LIMIT] Rate limit error before Continue button: ${continueCheck.errorText}`);
      const cleared = await waitForRateLimitErrorToClear(page, 20000, 3000);
      if (!cleared) {
        throw new Error(`Rate limit error before Continue: ${continueCheck.errorText}`);
      }
    }
    
    // 等待页面稳定，使用安全点击
    await waitForPageStable(page, 2000);
    const continueBtn = page.getByRole('button', { name: 'Continue' });
    await safeClick(page, continueBtn, { timeout: 10000, force: true, retries: 3 });
    
    // 点击后检查 429 错误页面（只有 429 才需要 disconnect）
    await page.waitForTimeout(1000);
    const continuePostErrorCheck = await checkIfErrorPage(page);
    if (continuePostErrorCheck.isErrorPage && continuePostErrorCheck.errorCode === '429') {
      console.warn(`[429-ERROR] Detected 429 error page after Continue, attempting recovery via disconnect...`);
      await handleErrorPage(page, BASE_URL);
      await page.waitForTimeout(2000);
    }
    
    // 点击后检查
    const afterContinueCheck = await checkPageForRateLimitError(page);
    if (afterContinueCheck.hasError) {
      console.warn(`[RATE-LIMIT] Rate limit error after Continue: ${afterContinueCheck.errorText}`);
      const cleared = await waitForRateLimitErrorToClear(page, 20000, 3000);
      if (!cleared) {
        throw new Error(`Rate limit error after Continue: ${afterContinueCheck.errorText}`);
      }
    }
    
    // 等待页面稳定，使用安全点击
    await waitForPageStable(page, 2000);
    const nextBtn1 = page.getByRole('button', { name: 'Next' });
    await safeClick(page, nextBtn1, { timeout: 10000, force: true, retries: 3 });

    // 等 connecting 结束（改为文本检测，避免无效的 :has-text 选择器）
    await page.waitForFunction(() => {
      const text = (document.body?.innerText || '').toLowerCase();
      // 若页面包含 "connecting" 视为未结束；否则结束
      return !text.includes('connecting');
    }, { timeout: 30_000 }).catch(() => { /* 容忍页面未出现该文案 */ });

    // 点击 Next 前检查 429 错误页面（只有 429 才需要 disconnect）
    const nextPreErrorCheck = await checkIfErrorPage(page);
    if (nextPreErrorCheck.isErrorPage && nextPreErrorCheck.errorCode === '429') {
      console.warn(`[429-ERROR] Detected 429 error page before Next, attempting recovery via disconnect...`);
      await handleErrorPage(page, BASE_URL);
      await page.waitForTimeout(2000);
    }
    
    // 点击 Next 前检查错误
    const nextCheck = await checkPageForRateLimitError(page);
    if (nextCheck.hasError) {
      console.warn(`[RATE-LIMIT] Rate limit error before Next button: ${nextCheck.errorText}`);
      const cleared = await waitForRateLimitErrorToClear(page, 20000, 3000);
      if (!cleared) {
        throw new Error(`Rate limit error before Next: ${nextCheck.errorText}`);
      }
    }

    // 等待页面稳定，使用安全点击
    await waitForPageStable(page, 2000);
    const nextBtn2 = page.getByRole('button', { name: 'Next' });
    await safeClick(page, nextBtn2, { timeout: 10000, force: true, retries: 3 });
    
    // 点击后检查 429 错误页面（只有 429 才需要 disconnect）
    await page.waitForTimeout(1000);
    const nextPostErrorCheck = await checkIfErrorPage(page);
    if (nextPostErrorCheck.isErrorPage && nextPostErrorCheck.errorCode === '429') {
      console.warn(`[429-ERROR] Detected 429 error page after Next, attempting recovery via disconnect...`);
      await handleErrorPage(page, BASE_URL);
      await page.waitForTimeout(2000);
    }
    
    // 点击后检查
    const afterNextCheck = await checkPageForRateLimitError(page);
    if (afterNextCheck.hasError) {
      console.warn(`[RATE-LIMIT] Rate limit error after Next: ${afterNextCheck.errorText}`);
      const cleared = await waitForRateLimitErrorToClear(page, 20000, 3000);
      if (!cleared) {
        throw new Error(`Rate limit error after Next: ${afterNextCheck.errorText}`);
      }
    }

    // 下个页面点击 check（更健壮：跨 frame 多文案兜底 + 重试）
    const clickCheckButton = async () => {
      const variants = [
        /^(check)$/i,
        /check\s*address/i,
        /check\s*eligibility/i,
        /verify/i
      ];
      const tryInFrame = async (frame) => {
        for (const rx of variants) {
          const byRole = frame.getByRole('button', { name: rx }).first();
          if (await byRole.isVisible().catch(() => false)) {
            // 等待 frame 稳定后再点击
            await frame.waitForTimeout(300);
            await byRole.click({ force: true });
            return true;
          }
          const byText = frame.locator('button').filter({ hasText: rx }).first();
          if (await byText.isVisible().catch(() => false)) {
            // 等待 frame 稳定后再点击
            await frame.waitForTimeout(300);
            await byText.click({ force: true });
            return true;
          }
          // CSS :has-text 作为兜底（转义特殊字符）
          const escapedPattern = rx.source.replace(/[\\"]/g, '\\$&');
          const css = frame.locator(`button:has-text("${escapedPattern}")`).first();
          if (await css.isVisible().catch(() => false)) {
            // 等待 frame 稳定后再点击
            await frame.waitForTimeout(300);
            await css.click({ force: true });
            return true;
          }
        }
        return false;
      };
      // 重试若干次，等待按钮出现
      for (let i = 0; i < 8; i++) {
        // 先主页面
        if (await tryInFrame(page)) return true;
        // 再所有子 frame
        for (const f of page.frames()) {
          if (f === page.mainFrame()) continue;
          if (await tryInFrame(f)) return true;
        }
        await page.waitForTimeout(1500);
      }
      return false;
    };
    const clickedCheck = await clickCheckButton();
    if (!clickedCheck) {
      // 如果未找到 Check，尝试检测是否已直接进入 Terms 页面，则跳过该步骤继续
      const hasTerms = await (async () => {
        const probe = async (frame) => frame.evaluate(() => {
          const text = document.body?.innerText || '';
          return /Accept Token End User Terms/i.test(text) || /By checking this box/i.test(text) || /have read and understood the Glacier Drop terms/i.test(text);
        }).catch(() => false);
        if (await probe(page)) return true;
        for (const f of page.frames()) {
          if (f === page.mainFrame()) continue;
          if (await probe(f)) return true;
        }
        return false;
      })();
      if (!hasTerms) {
        throw new Error('Check button not found');
      }
    }

    // 若进入 Terms 页面：提取 hex，用本地签名服务签名；滚动到底部，勾选复选框并接受
    let minedHex = null; // 在外层定义，后续签名页也要用到
    let signDataTerms = null; // 在 Terms 页面获取的签名数据
    let termsSigned = false; // 标记是否已在 Terms 页面成功签名
    try {
      // 仅当看到页面标题或关键文案时才执行（主文档或子 frame）
      const findTermsFrame = async () => {
        // 主页面先检查
        const hasInMain = await page.evaluate(() => {
          const text = document.body?.innerText || '';
          return /Accept Token End User Terms/i.test(text) || /By checking this box/i.test(text);
        }).catch(() => false);
        if (hasInMain) return page;
        // 子 frame 检查
        for (const f of page.frames()) {
          if (f === page.mainFrame()) continue;
          try {
            const found = await f.evaluate(() => {
              const text = document.body?.innerText || '';
              return /Accept Token End User Terms/i.test(text) || /By checking this box/i.test(text);
            });
            if (found) return f;
          } catch {}
        }
        return null;
      };

      const host = await findTermsFrame();
      if (!host) throw new Error('Terms page not detected');

      // 提取“Copy to Clipboard”区域中的 mining process: 后的 hex
      minedHex = await (async () => {
        try {
          const text = await host.evaluate(() => document.body?.innerText || '');
          const m = text.match(/mining\s+process:\s*([0-9a-f]{64,})/i);
          return m ? m[1].toLowerCase() : null;
        } catch { return null; }
      })();
      // 若未提取到，尝试从可能的代码块/复制源附近查找
      if (!minedHex) {
        minedHex = await host.evaluate(() => {
          const candidates = Array.from(document.querySelectorAll('pre, code, textarea, [data-copy], .copy, .clipboard, [data-clipboard-text]'));
          for (const el of candidates) {
            const t = (el.getAttribute('data-clipboard-text') || el.textContent || '').trim();
            const m = t.match(/mining\s+process:\s*([0-9a-f]{64,})/i);
            if (m) return m[1].toLowerCase();
          }
          return null;
        }).catch(() => null);
      }
      // 如果仍没有，稍后在签名页再尝试一次，不阻塞流程

      // 优化：快速滚动到底部（简化逻辑，减少等待）
      const quickScrollToBottom = async () => {
        // 使用 locator 穿透 shadow 定位到标题元素
        const headerLoc = host.getByText(/TOKEN END-USER TERMS/i).first();
        const headerEl = await headerLoc.elementHandle().catch(() => null);
        if (!headerEl) {
          // 找不到标题，则尝试滚动整个页面
          await host.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
          return false;
        }
        // 在页面端寻找可滚动祖先（跨越 shadow root）
        const reachedBottom = await host.evaluate((el) => {
          function getScrollParentDeep(node) {
            let current = node;
            // 优化：减少循环次数，50 次足够
            for (let i = 0; i < 50 && current; i++) {
              const style = current instanceof Element ? getComputedStyle(current) : null;
              if (style) {
                const oy = style.overflowY;
                const canScroll = (oy === 'auto' || oy === 'scroll') && current.scrollHeight > current.clientHeight + 2;
                if (canScroll) return current;
              }
              // 向上一个常规父元素
              if (current.parentElement) { current = current.parentElement; continue; }
              // 穿过 shadow boundary
              const root = current.getRootNode && current.getRootNode();
              if (root && root.host) { current = root.host; continue; }
              break;
            }
            return null;
          }
          const panel = getScrollParentDeep(el) || document.scrollingElement || document.documentElement;
          // 给容器聚焦，模拟真实滚轮事件（有些站点只接受 wheel 触发）
          if (panel instanceof HTMLElement) { panel.focus && panel.focus(); }
          const target = panel;
          function atBottom() {
            return Math.abs(target.scrollHeight - target.clientHeight - target.scrollTop) < 4;
          }
          // 优化：直接滚动到底，减少循环次数
          target.scrollTop = target.scrollHeight;
          const rect = (target instanceof Element ? target.getBoundingClientRect() : document.body.getBoundingClientRect());
          const wheel = new WheelEvent('wheel', {
            bubbles: true, cancelable: true, deltaY: 10000,
            clientX: rect.left + rect.width / 2, clientY: rect.top + rect.height / 2
          });
          (target instanceof Element ? target : document.body).dispatchEvent(wheel);
          return atBottom();
        }, headerEl).catch(() => false);

        // 优化：快速鼠标滚轮和键盘滚动（一次性完成）
        try {
          const box = await headerLoc.boundingBox().catch(() => null);
          if (box) {
            await host.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
            await host.mouse.wheel(0, 5000); // 一次性大滚动
          }
          await host.keyboard.press('End');
        } catch {}
        return reachedBottom;
      };

      // 优化：同时滚动所有可滚动容器作为兜底（并行执行）
      await host.evaluate(() => {
        Array.from(document.querySelectorAll('*'))
          .filter(el => {
            const style = getComputedStyle(el);
            return (style.overflowY === 'auto' || style.overflowY === 'scroll') && 
                   el.scrollHeight > el.clientHeight;
          })
          .forEach(el => { el.scrollTop = el.scrollHeight; });
      }).catch(() => {});

      // 优化：简化循环，只尝试一次快速滚动
      await quickScrollToBottom();
      await host.waitForTimeout(100); // 减少等待时间

      // 在 Terms 页面直接完成签名字段填充（Public key / Signature），避免卡在此页
      if (minedHex) {
        try {
          signDataTerms = await signWithRateLimit(task.addr, minedHex);
          termsSigned = true; // 标记已在 Terms 页面成功签名
          
          if (signDataTerms) {
            // 将 publicKeyHex 和 coseSign1Hex 填入（作用于 host frame 内）
            const fillInHost = async (labelRegex, value) => {
              // 在浏览器端做深度遍历（含 shadowRoot），根据 label 文本或 aria/name/placeholder 匹配
              const found = await host.evaluate((regexSource, val) => {
                const labelRe = new RegExp(regexSource, 'i');
                function* walk(node) {
                  if (!node) return;
                  yield node;
                  if (node.shadowRoot) {
                    for (const c of node.shadowRoot.querySelectorAll('*')) yield c;
                  }
                  if (node.children) {
                    for (const c of node.children) yield* walk(c);
                  }
                }
                function setVal(el, v) {
                  const proto = Object.getPrototypeOf(el);
                  const desc = Object.getOwnPropertyDescriptor(proto, 'value');
                  if (desc && desc.set) desc.set.call(el, '');
                  el.value = '';
                  el.dispatchEvent(new Event('input', { bubbles: true }));
                  el.value = v;
                  el.dispatchEvent(new Event('input', { bubbles: true }));
                  el.dispatchEvent(new Event('change', { bubbles: true }));
                }
                // 尝试直接通过可访问性属性匹配
                const q = '[name],[aria-label],[placeholder]';
                const all = Array.from(document.querySelectorAll(`input${q},textarea${q}`));
                let target = all.find(el => labelRe.test(el.getAttribute('aria-label')||'') || labelRe.test(el.getAttribute('name')||'') || labelRe.test(el.getAttribute('placeholder')||''));
                // 精确命中截图里的占位符（如果是查找 Public key）
                if (!target && /public\s*key/i.test(regexSource)) {
                  target = document.querySelector('input[placeholder="Please enter a public key"], textarea[placeholder="Please enter a public key"]');
                }
                if (!target) {
                  // 根据 label 文本邻近
                  const labels = Array.from(document.querySelectorAll('*')).filter(n => labelRe.test(n.textContent||''));
                  for (const lab of labels) {
                    const sibInput = lab.closest('*')?.querySelector('input,textarea');
                    if (sibInput) { target = sibInput; break; }
                  }
                }
                if (!target) {
                  // 精确：若 label 有 for 属性，按 id 取 input
                  const labelEl = Array.from(document.querySelectorAll('label'))
                    .find(l => labelRe.test(l.textContent||''));
                  const forId = labelEl && labelEl.getAttribute('for');
                  if (forId) {
                    const byId = document.getElementById(forId);
                    if (byId && (byId instanceof HTMLInputElement || byId instanceof HTMLTextAreaElement)) {
                      target = byId;
                    }
                  }
                }
                if (!target) {
                  // 深度遍历 shadowRoot 查找
                  for (const n of walk(document.body)) {
                    if (n instanceof HTMLInputElement || n instanceof HTMLTextAreaElement) {
                      const a = (n.getAttribute('aria-label')||'') + ' ' + (n.getAttribute('name')||'') + ' ' + (n.getAttribute('placeholder')||'');
                      if (labelRe.test(a)) { target = n; break; }
                    }
                    // 兼容 contenteditable 容器
                    if (n instanceof HTMLElement && n.getAttribute && n.getAttribute('contenteditable') === 'true') {
                      const a = (n.getAttribute('aria-label')||'') + ' ' + (n.getAttribute('name')||'') + ' ' + (n.getAttribute('placeholder')||'');
                      if (labelRe.test(a)) { target = n; break; }
                    }
                  }
                }
                if (!target) return false;
                target.scrollIntoView({ block: 'center', behavior: 'instant' });
                if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) {
                  setVal(target, val);
                } else if (target instanceof HTMLElement && target.getAttribute('contenteditable') === 'true') {
                  target.focus();
                  target.textContent = '';
                  target.dispatchEvent(new Event('input', { bubbles: true }));
                  target.textContent = val;
                  target.dispatchEvent(new Event('input', { bubbles: true }));
                  target.dispatchEvent(new Event('change', { bubbles: true }));
                }
                return true;
              }, labelRegex.source, value).catch(() => false);
              // 若浏览器端未能设置，回退到 Playwright 键入
              if (!found) {
                const loc = host.locator('input[placeholder="Please enter a public key"], textarea[placeholder="Please enter a public key"]').first();
                if (await loc.isVisible().catch(() => false)) {
                  await loc.click({ timeout: 2000 }).catch(() => {});
                  try { await loc.fill(''); } catch {}
                  try { await host.keyboard.type(value, { delay: 2 }); } catch {}
                  return true;
                }
              }
              return !!found;
            };

            // 滚动容器函数：滚动所有可滚动容器以确保内容可见
            const doScrollContainers = async () => {
              await host.evaluate(() => {
                Array.from(document.querySelectorAll('*'))
                  .filter(el => {
                    const style = getComputedStyle(el);
                    return (style.overflowY === 'auto' || style.overflowY === 'scroll') && 
                           el.scrollHeight > el.clientHeight;
                  })
                  .forEach(el => { el.scrollTop = el.scrollHeight; });
              }).catch(() => {});
            };

            // 滚动容器，确保输入框出现
            await doScrollContainers();
            
            // 使用优化后的统一函数填写 Public key
            console.log('[DEBUG] Filling Public key on Terms page...');
            const filledPk = await fillInputFieldOptimized(
              host,
              /public\s*key/i,
              signDataTerms.publicKeyHex,
              { inputType: 'input', verifyAfter: true }
            );
            console.log('[DEBUG] Public key filled:', filledPk);
            
            // 使用优化后的统一函数填写 Signature
            await doScrollContainers();
            console.log('[DEBUG] Filling Signature on Terms page...');
            const filledSig = await fillInputFieldOptimized(
              host,
              /^(\s*)signature(\s*)$/i,
              signDataTerms.coseSign1Hex,
              { inputType: 'textarea', verifyAfter: true }
            );
            console.log('[DEBUG] Signature filled:', filledSig);

            // 触发校验：对两个输入派发 blur，并等待页面校验完成
            await host.waitForTimeout(500); // 给页面一点时间处理输入
            await host.evaluate(() => {
              const pk = document.getElementById('input');
              const sig = document.querySelector('textarea') || Array.from(document.querySelectorAll('textarea')).find(t => t.value && t.value.length > 100);
              const blur = (el) => el && el.dispatchEvent(new FocusEvent('blur', { bubbles: true }));
              if (pk) { blur(pk); pk.dispatchEvent(new Event('change', { bubbles: true })); }
              if (sig) { blur(sig); sig.dispatchEvent(new Event('change', { bubbles: true })); }
            }).catch(() => {});
            
            // 等待页面校验逻辑完成，Sign 按钮变为可用
            console.log('[DEBUG] Waiting for Sign button to become enabled...');
            await host.waitForFunction(() => {
              const btn = Array.from(document.querySelectorAll('button')).find(b => /^(\s*)sign(\s*)$/i.test(b.textContent || ''));
              if (!btn) return false;
              const ariaDis = btn.getAttribute('aria-disabled');
              const isEnabled = !btn.disabled && ariaDis !== 'true';
              if (isEnabled) console.log('Sign button is now enabled!');
              return isEnabled;
            }, { timeout: 5000 }).catch(() => {  // 优化：减少超时时间
              console.log('[DEBUG] Sign button did not become enabled, checking current state...');
            });

            const signBtnHost = host.getByRole('button', { name: /^sign$/i }).first();
            if (await signBtnHost.isVisible().catch(() => false)) {
              await signBtnHost.scrollIntoViewIfNeeded().catch(() => {});
              await signBtnHost.click().catch(() => {});
            }
          }
        } catch {}
      }

      // 勾选复选框：多种策略，确保checkbox被正确选中
      const checkCheckbox = async () => {
        // 策略1: 通过ID直接查找（HTML中id="accept-terms"）
        try {
          const byId = host.locator('#accept-terms');
          if (await byId.isVisible({ timeout: 2000 }).catch(() => false)) {
            const isChecked = await byId.isChecked().catch(() => false);
            if (!isChecked) {
              await byId.check({ force: true });
              // 验证是否选中成功
              await host.waitForTimeout(200);
              const verifyChecked = await byId.isChecked().catch(() => false);
              if (verifyChecked) {
                console.log('[TERMS] Checkbox checked via ID (#accept-terms)');
                return true;
              }
            } else {
              console.log('[TERMS] Checkbox already checked via ID');
              return true;
            }
          }
        } catch {}
        
        // 策略2: 通过 label 关联查找
        try {
          const byLabel = host.getByLabel(/By checking this box|accept.*terms|read.*understood/i);
          if (await byLabel.first().isVisible({ timeout: 2000 }).catch(() => false)) {
            const isChecked = await byLabel.first().isChecked().catch(() => false);
            if (!isChecked) {
          await byLabel.first().check({ force: true });
              await host.waitForTimeout(200);
              const verifyChecked = await byLabel.first().isChecked().catch(() => false);
              if (verifyChecked) {
                console.log('[TERMS] Checkbox checked via label');
          return true;
        }
            } else {
              console.log('[TERMS] Checkbox already checked via label');
            return true;
          }
        }
        } catch {}
        
        // 策略3: 通过文本查找附近的checkbox
        try {
          const nearText = host.getByText(/By checking this box|read.*understood.*Glacier Drop/i);
          if (await nearText.first().isVisible({ timeout: 2000 }).catch(() => false)) {
            // 查找label或附近的checkbox
            const container = nearText.first().locator('xpath=ancestor::label | ..');
            const cb = container.locator('input[type="checkbox"], [role="checkbox"]').first();
            if (await cb.isVisible({ timeout: 1000 }).catch(() => false)) {
              const isChecked = await cb.isChecked().catch(() => false);
              if (!isChecked) {
                try { 
                  await cb.check({ force: true });
                } catch { 
                  await cb.click({ force: true });
                }
                await host.waitForTimeout(200);
                const verifyChecked = await cb.isChecked().catch(() => false);
                if (verifyChecked) {
                  console.log('[TERMS] Checkbox checked via text near checkbox');
          return true;
        }
              } else {
                console.log('[TERMS] Checkbox already checked via text');
                return true;
              }
            }
          }
        } catch {}
        
        // 策略4: 直接查找任何可见的checkbox
        try {
          const anyCb = host.locator('input[type="checkbox"][id*="accept"], input[type="checkbox"][name*="accept"], input[type="checkbox"]').first();
          if (await anyCb.isVisible({ timeout: 2000 }).catch(() => false)) {
            const isChecked = await anyCb.isChecked().catch(() => false);
            if (!isChecked) {
              try { 
                await anyCb.check({ force: true });
              } catch { 
                await anyCb.click({ force: true });
              }
              await host.waitForTimeout(200);
              const verifyChecked = await anyCb.isChecked().catch(() => false);
              if (verifyChecked) {
                console.log('[TERMS] Checkbox checked via direct locator');
                return true;
              }
            } else {
              console.log('[TERMS] Checkbox already checked via direct locator');
              return true;
            }
          }
        } catch {}
        
        return false;
      };
      
      // 多次尝试选中checkbox，直到成功
      let checked = false;
      for (let attempt = 1; attempt <= 5; attempt++) {
        checked = await checkCheckbox();
        if (checked) break;
        
        if (attempt < 5) {
          console.log(`[TERMS] Checkbox check failed, retrying (attempt ${attempt}/5)...`);
          await host.waitForTimeout(300);
        }
      }
      
      if (!checked) {
        console.error('[TERMS] Failed to check checkbox after all attempts');
        throw new Error('Terms checkbox not found or could not be checked');
      }
      
      // 最终验证：确保checkbox已选中
      await host.waitForTimeout(300);
      const finalCheck = await (async () => {
        try {
          const byId = host.locator('#accept-terms');
          if (await byId.isVisible({ timeout: 1000 }).catch(() => false)) {
            return await byId.isChecked().catch(() => false);
          }
          const anyCb = host.locator('input[type="checkbox"]').first();
          if (await anyCb.isVisible({ timeout: 1000 }).catch(() => false)) {
            return await anyCb.isChecked().catch(() => false);
          }
          return false;
        } catch {
          return false;
        }
      })();
      
      if (!finalCheck) {
        console.warn('[TERMS] Final checkbox verification failed, but continuing anyway...');
      } else {
        console.log('[TERMS] ✓ Checkbox verified as checked');
      }

      // 等待按钮变为可用（checkbox选中后，按钮可能需要一些时间才能启用）
      const acceptBtn = host.getByRole('button', { name: /Accept and Sign/i }).first();
      await acceptBtn.scrollIntoViewIfNeeded().catch(() => {});
      
      // 等待按钮启用（最多等待3秒）
      let buttonEnabled = false;
      for (let i = 0; i < 6; i++) {
        const isVisible = await acceptBtn.isVisible({ timeout: 1000 }).catch(() => false);
        const isDisabled = await acceptBtn.isDisabled({ timeout: 1000 }).catch(() => true);
        
        if (isVisible && !isDisabled) {
          buttonEnabled = true;
          console.log('[TERMS] Accept and Sign button is enabled');
          break;
        }
        
        if (i < 5) {
          console.log(`[TERMS] Waiting for Accept and Sign button to be enabled (${i + 1}/6)...`);
        try { await host.keyboard.press('End'); } catch {}
          await host.waitForTimeout(500);
          
          // 如果按钮仍然禁用，再次检查checkbox状态
          if (isDisabled) {
            console.log('[TERMS] Button still disabled, re-checking checkbox...');
            // 重新定义checkCheckbox函数（因为可能在外部作用域）
            const recheckCheckbox = async () => {
              try {
                const byId = host.locator('#accept-terms');
                if (await byId.isVisible({ timeout: 1000 }).catch(() => false)) {
                  const isChecked = await byId.isChecked().catch(() => false);
                  if (!isChecked) {
                    await byId.check({ force: true });
                    await host.waitForTimeout(200);
                    return await byId.isChecked().catch(() => false);
                  }
                  return true;
                }
                const anyCb = host.locator('input[type="checkbox"]').first();
                if (await anyCb.isVisible({ timeout: 1000 }).catch(() => false)) {
                  const isChecked = await anyCb.isChecked().catch(() => false);
                  if (!isChecked) {
                    try { await anyCb.check({ force: true }); } catch { await anyCb.click({ force: true }); }
                    await host.waitForTimeout(200);
                    return await anyCb.isChecked().catch(() => false);
                  }
                  return true;
                }
                return false;
              } catch {
                return false;
              }
            };
            const rechecked = await recheckCheckbox();
            if (rechecked) {
              console.log('[TERMS] ✓ Checkbox re-checked successfully');
              await host.waitForTimeout(300);
            } else {
              console.warn('[TERMS] Failed to re-check checkbox');
            }
          }
        }
      }
      
      if (!buttonEnabled) {
        console.warn('[TERMS] Accept and Sign button may still be disabled, will try to click anyway');
      }
      
      // 尝试点击 Accept and Sign 并验证离开 Terms 页面（最多重试 3 次）
      let leftTermsPage = false;
      const maxRetries = 3;
      
      for (let attempt = 1; attempt <= maxRetries; attempt++) {
        console.log(`[TERMS] Attempting to click "Accept and Sign" (attempt ${attempt}/${maxRetries})...`);
        
        // 等待页面稳定，使用安全点击
        await waitForPageStable(page, 2000);
        
        // 重新获取按钮（可能因为页面变化而失效）
        const acceptBtnRetry = host.getByRole('button', { name: /Accept and Sign/i }).first();
        const isVisible = await acceptBtnRetry.isVisible({ timeout: 3000 }).catch(() => false);
        
        if (!isVisible) {
          console.warn(`[TERMS] "Accept and Sign" button not visible on attempt ${attempt}`);
          // 可能已经跳转了，检查一下
          const checkStillInTerms = await (async () => {
            try {
              const text = await page.evaluate(() => document.body?.innerText || '').catch(() => '');
              if (/Accept Token End User Terms/i.test(text) || /TOKEN END-USER TERMS/i.test(text)) {
                return await acceptBtnRetry.isVisible({ timeout: 500 }).catch(() => false);
              }
              return false;
            } catch {
              return false;
            }
          })();
          
          if (!checkStillInTerms) {
            console.log('[TERMS] Button not visible, may have already left Terms page');
            leftTermsPage = true;
            break;
          }
          
          if (attempt < maxRetries) {
            await page.waitForTimeout(2000);
            continue;
          }
        }
        
        // 滚动到按钮位置
        await acceptBtnRetry.scrollIntoViewIfNeeded().catch(() => {});
        await host.waitForTimeout(300);
        
        // 点击按钮
        try {
          await safeClick(page, acceptBtnRetry, { timeout: 10000, force: true, retries: 2 });
    } catch (e) {
          console.warn(`[TERMS] Failed to click button on attempt ${attempt}: ${e.message}`);
          if (attempt < maxRetries) {
            await page.waitForTimeout(2000);
            continue;
          }
        }
        
        console.log(`[TERMS] Clicked "Accept and Sign" (attempt ${attempt}), waiting for page to leave Terms page...`);
        
        // 等待并验证是否成功离开 Terms 页面（最多等待 15 秒）
        leftTermsPage = await (async () => {
          const maxWait = 15000;
          const startTime = Date.now();
          const checkInterval = 500;
          
          while (Date.now() - startTime < maxWait) {
            // 检查是否还在 Terms 页面
            const stillInTerms = await (async () => {
              try {
                // 等待一下，让页面有时间跳转
                await page.waitForTimeout(300);
                
                const text = await page.evaluate(() => document.body?.innerText || '').catch(() => '');
                // 检查是否还有 Terms 页面的特征文本
                if (/Accept Token End User Terms/i.test(text) || /TOKEN END-USER TERMS/i.test(text)) {
                  // 再检查是否还有 Accept and Sign 按钮（可能还没跳转）
                  const hasAcceptBtn = await host.getByRole('button', { name: /Accept and Sign/i }).first().isVisible({ timeout: 500 }).catch(() => false);
                  if (hasAcceptBtn) {
                    return true; // 还在 Terms 页面
                  }
                }
                
                // 检查所有 frame
                for (const f of page.frames()) {
                  try {
                    const frameText = await f.evaluate(() => document.body?.innerText || '').catch(() => '');
                    if (/Accept Token End User Terms/i.test(frameText) || /TOKEN END-USER TERMS/i.test(frameText)) {
                      const frameAcceptBtn = f.getByRole('button', { name: /Accept and Sign/i }).first();
                      if (await frameAcceptBtn.isVisible({ timeout: 500 }).catch(() => false)) {
                        return true; // 还在 Terms 页面
                      }
                    }
                  } catch {}
                }
                
                return false; // 已经离开 Terms 页面
              } catch (e) {
                return false; // 检查失败，假设已离开
              }
            })();
            
            if (!stillInTerms) {
              console.log(`[TERMS] ✓ Successfully left Terms page (after ${Date.now() - startTime}ms)`);
              await page.waitForTimeout(1000); // 额外等待 1 秒确保页面稳定
              return true;
            }
            
            await page.waitForTimeout(checkInterval);
          }
          
          return false; // 超时，可能还在 Terms 页面
        })();
        
        if (leftTermsPage) {
          break; // 成功离开，退出重试循环
        } else {
          console.warn(`[TERMS] Still in Terms page after attempt ${attempt}, will retry...`);
          if (attempt < maxRetries) {
            await page.waitForTimeout(3000); // 等待更长时间再重试
          }
        }
      }
      
      if (!leftTermsPage) {
        console.error('[TERMS] Failed to leave Terms page after all retries');
        // 不立即抛出错误，先尝试继续执行，看看是否能从后续流程恢复
      }
    } catch (e) {
      console.error(`[TERMS] Error processing Terms page: ${e.message}`);
      // 不抛出错误，继续执行，看看是否能从签名页面继续
    }

    // 进入签名页：若未取到 hex，再次从页面复制块中抓取
    if (!minedHex) {
      try {
        await page.waitForTimeout(200); // 优化：减少等待时间
        const text2 = await page.evaluate(() => document.body?.innerText || '');
        const m2 = text2.match(/mining\s+process:\s*([0-9a-f]{64,})/i);
        if (m2) minedHex = m2[1].toLowerCase();
      } catch {}
    }

    if (!minedHex) {
      throw new Error('Failed to extract mining process hex');
    }

    // 请求本地签名服务（如果 Terms 页面已经签名过，复用数据，避免重复请求）
    let signData;
    if (termsSigned && signDataTerms) {
      console.log('[SIGN] Reusing signature data from Terms page to avoid duplicate request');
      signData = signDataTerms;
    } else {
      signData = await signWithRateLimit(task.addr, minedHex);
    }

    // 将 publicKeyHex 和 coseSign1Hex 填入页面
    const fillIfExists = async (labelRegex, value) => {
      const byLabel = page.getByLabel(labelRegex).first();
      if (await byLabel.isVisible().catch(() => false)) {
        await byLabel.fill(value);
        return true;
      }
      const byPh = page.getByPlaceholder(labelRegex).first();
      if (await byPh.isVisible().catch(() => false)) {
        await byPh.fill(value);
        return true;
      }
      const inputNear = page.getByText(labelRegex).first().locator('..').locator('input, textarea').first();
      if (await inputNear.isVisible().catch(() => false)) {
        await inputNear.fill(value);
        return true;
      }
      return false;
    };

    // 使用优化后的统一函数填写 Public key
    console.log('[DEBUG] Filling Public key on sign page...');
    let pkFilled = await fillInputFieldOptimized(
      page,
      /public\s*key/i,
      signData.publicKeyHex,
      { inputType: 'input', verifyAfter: true }
    );
    console.log('[DEBUG] Public key filled on sign page:', pkFilled);
    
    // 使用优化后的统一函数填写 Signature
    console.log('[DEBUG] Filling Signature on sign page...');
    let sigFilled = await fillInputFieldOptimized(
      page,
      /^(\s*)signature(\s*)$/i,
      signData.coseSign1Hex,
      { inputType: 'textarea', verifyAfter: true }
    );
    console.log('[DEBUG] Signature filled on sign page:', sigFilled);
    
    // 如果填写失败，尝试在所有 frame 中填写（更积极的策略）
    let pkFilledResult = pkFilled;
    let sigFilledResult = sigFilled;
    
    if (!pkFilledResult || !sigFilledResult) {
      console.log('[DEBUG] Retrying in all frames with more aggressive strategies...');
      
      // 等待页面稳定
      await page.waitForTimeout(500);
      
      // 尝试所有 frame（包括主页面）
      const allFrames = [page, ...page.frames()];
      for (const frame of allFrames) {
        try {
          if (!pkFilledResult) {
            const result = await fillInputFieldOptimized(frame, /public\s*key/i, signData.publicKeyHex, { 
              inputType: 'input', 
              verifyAfter: false,
              timeout: 3000 
            });
            if (result) {
              console.log('[DEBUG] Public key filled successfully in frame');
              pkFilledResult = true;
            }
          }
          if (!sigFilledResult) {
            const result = await fillInputFieldOptimized(frame, /^(\s*)signature(\s*)$/i, signData.coseSign1Hex, { 
              inputType: 'textarea', 
              verifyAfter: false,
              timeout: 3000 
            });
            if (result) {
              console.log('[DEBUG] Signature filled successfully in frame');
              sigFilledResult = true;
            }
          }
          
          // 如果都填好了，立即退出，不要等待其他 frame
          if (pkFilledResult && sigFilledResult) {
            console.log('[DEBUG] Both fields filled successfully, exiting frame loop immediately');
            break;
          }
    } catch (e) {
          // 忽略单个 frame 的错误，继续尝试其他 frame
          console.warn(`[DEBUG] Error filling in frame: ${e.message}`);
        }
      }
      
      // 如果还是失败，尝试直接通过 DOM 操作填写
      if (!pkFilledResult || !sigFilledResult) {
        console.log('[DEBUG] Attempting direct DOM manipulation as last resort...');
        try {
          await page.evaluate(({ pkValue, sigValue }) => {
            // 查找所有 input 和 textarea
            const allInputs = Array.from(document.querySelectorAll('input, textarea'));
            
            // 查找 Public key 输入框
            for (const input of allInputs) {
              const label = input.closest('div, form, section')?.textContent || '';
              const placeholder = input.getAttribute('placeholder') || '';
              const ariaLabel = input.getAttribute('aria-label') || '';
              
              if (/public\s*key/i.test(label + placeholder + ariaLabel)) {
                input.value = pkValue;
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
                console.log('Direct DOM: Public key filled');
                break;
              }
            }
            
            // 查找 Signature 输入框
            for (const input of allInputs) {
              const label = input.closest('div, form, section')?.textContent || '';
              const placeholder = input.getAttribute('placeholder') || '';
              const ariaLabel = input.getAttribute('aria-label') || '';
              
              if (/^(\s*)signature(\s*)$/i.test(label + placeholder + ariaLabel)) {
                input.value = sigValue;
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
                console.log('Direct DOM: Signature filled');
                break;
              }
            }
          }, { pkValue: signData.publicKeyHex, sigValue: signData.coseSign1Hex });
          
          // 标记为已填写（即使不确定）
          pkFilledResult = true;
          sigFilledResult = true;
          console.log('[DEBUG] Direct DOM manipulation attempted');
    } catch (e) {
          console.warn(`[DEBUG] Direct DOM manipulation failed: ${e.message}`);
        }
      }
    }
    
    // 更新变量
    pkFilled = pkFilledResult;
    sigFilled = sigFilledResult;
    
    // 无论填写成功方式如何，都触发验证事件（确保页面知道值已改变）
    if (pkFilled && sigFilled) {
      console.log('[DEBUG] Triggering validation events after successful filling...');
      try {
        // 快速在所有 frame 中触发验证事件（使用 Promise.race 确保不卡住）
        const triggerEvents = async () => {
          const frames = [page, ...page.frames()];
          for (const frame of frames) {
            try {
              await Promise.race([
                frame.evaluate(() => {
                  const allInputs = Array.from(document.querySelectorAll('input, textarea'));
                  for (const input of allInputs) {
                    input.dispatchEvent(new Event('blur', { bubbles: true }));
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                  }
                }),
                new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 500))
              ]).catch(() => {});
            } catch {}
          }
        };
        
        // 最多等待1秒，然后继续（不阻塞）
        await Promise.race([
          triggerEvents(),
          new Promise((resolve) => setTimeout(resolve, 1000))
        ]).catch(() => {});
        console.log('[DEBUG] Validation events triggered');
      } catch {}
    }
    
    // 如果填写成功，直接等待一小段时间然后立即查找并点击 Sign 按钮
    if (pkFilled && sigFilled) {
      console.log('[DEBUG] Fields filled successfully, waiting briefly then proceeding to click Sign...');
      await page.waitForTimeout(800); // 给页面时间处理验证（800ms应该足够了）
    } else {
      // 如果填写失败，才进行验证和重填
      console.log('[DEBUG] Filling failed, performing quick verification...');
          await page.waitForTimeout(300);
      
      // 快速验证（最多等待1秒）
      const finalCheck = await Promise.race([
        (async () => {
          try {
            // 快速检查第一个 frame（通常Sign页面在frame中）
            if (page.frames().length > 0) {
              const frame = page.frames()[0];
              const check = await frame.evaluate(({ pkExpected, sigExpected }) => {
                let pkFound = false, sigFound = false;
                const allInputs = Array.from(document.querySelectorAll('input, textarea'));
                for (const input of allInputs) {
                  const label = input.closest('div, form, section')?.textContent || '';
                  const placeholder = input.getAttribute('placeholder') || '';
                  const ariaLabel = input.getAttribute('aria-label') || '';
                  if (/public\s*key/i.test(label + placeholder + ariaLabel) && input.value === pkExpected) pkFound = true;
                  if (/^(\s*)signature(\s*)$/i.test(label + placeholder + ariaLabel) && input.value === sigExpected) sigFound = true;
                }
                return { pkOk: pkFound, sigOk: sigFound };
              }, { pkExpected: signData.publicKeyHex, sigExpected: signData.coseSign1Hex }).catch(() => ({ pkOk: false, sigOk: false }));
              if (check.pkOk && check.sigOk) return check;
            }
            // 检查主页面
            const check = await page.evaluate(({ pkExpected, sigExpected }) => {
              let pkFound = false, sigFound = false;
              const allInputs = Array.from(document.querySelectorAll('input, textarea'));
              for (const input of allInputs) {
                const label = input.closest('div, form, section')?.textContent || '';
                const placeholder = input.getAttribute('placeholder') || '';
                const ariaLabel = input.getAttribute('aria-label') || '';
                if (/public\s*key/i.test(label + placeholder + ariaLabel) && input.value === pkExpected) pkFound = true;
                if (/^(\s*)signature(\s*)$/i.test(label + placeholder + ariaLabel) && input.value === sigExpected) sigFound = true;
              }
              return { pkOk: pkFound, sigOk: sigFound };
            }, { pkExpected: signData.publicKeyHex, sigExpected: signData.coseSign1Hex }).catch(() => ({ pkOk: false, sigOk: false }));
            return check;
          } catch {
            return { pkOk: false, sigOk: false };
          }
        })(),
        new Promise((resolve) => setTimeout(() => resolve({ pkOk: false, sigOk: false }), 1000))
      ]).catch(() => ({ pkOk: false, sigOk: false }));
      
      if ((!finalCheck.pkOk || !finalCheck.sigOk) && (!pkFilled || !sigFilled)) {
        console.log('[DEBUG] Verification failed, attempting refill...');
        if (!finalCheck.pkOk) {
          await fillInputFieldOptimized(page, /public\s*key/i, signData.publicKeyHex, { inputType: 'input', verifyAfter: false });
        }
        if (!finalCheck.sigOk) {
          await fillInputFieldOptimized(page, /^(\s*)signature(\s*)$/i, signData.coseSign1Hex, { inputType: 'textarea', verifyAfter: false });
        }
        await page.waitForTimeout(500);
      }
    }
    
    // 查找 Sign 按钮（在所有可能的位置）
    console.log('[DEBUG] Looking for Sign button to click...');
    let signBtn = null;
    let signBtnFrame = null;
    
    // 策略1: 在主页面查找
    try {
      const mainSignBtn = page.locator('button:has-text("Sign")').first();
      if (await mainSignBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
        signBtn = mainSignBtn;
        signBtnFrame = page;
        console.log('[DEBUG] Found Sign button on main page');
      }
    } catch {}
    
    // 策略2: 如果主页面没找到，在所有 frame 中查找
    if (!signBtn) {
      for (const frame of page.frames()) {
        try {
          const frameSignBtn = frame.locator('button:has-text("Sign")').first();
          if (await frameSignBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
            signBtn = frameSignBtn;
            signBtnFrame = frame;
            console.log('[DEBUG] Found Sign button in frame');
                break;
              }
        } catch {}
      }
    }
    
    // 策略3: 使用 role 定位
    if (!signBtn) {
      try {
        const roleSignBtn = page.getByRole('button', { name: /^sign$/i }).first();
        if (await roleSignBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
          signBtn = roleSignBtn;
          signBtnFrame = page;
          console.log('[DEBUG] Found Sign button via role on main page');
        }
      } catch {}
    }
    
    if (!signBtn) {
      console.error('[DEBUG] Sign button not found! Trying to find it via evaluate...');
      // 最后尝试：通过 evaluate 查找并滚动到按钮
      await page.evaluate(() => {
        const btn = Array.from(document.querySelectorAll('button')).find(b => /^(\s*)sign(\s*)$/i.test(b.textContent || ''));
        if (btn) {
          btn.scrollIntoView({ behavior: 'smooth', block: 'center' });
          // 尝试启用按钮
          btn.disabled = false;
          btn.removeAttribute('disabled');
          btn.setAttribute('aria-disabled', 'false');
        }
      }).catch(() => {});
      
      // 再次尝试查找
      signBtn = page.locator('button:has-text("Sign")').first();
      signBtnFrame = page;
    }
    
    if (signBtn) {
      // 检查按钮是否禁用，如果禁用尝试启用
      try {
        const isDisabled = await signBtn.isDisabled().catch(() => true);
        if (isDisabled) {
          console.log('[DEBUG] Sign button is disabled, attempting to enable it...');
          await (signBtnFrame || page).evaluate(() => {
      const btn = Array.from(document.querySelectorAll('button')).find(b => /^(\s*)sign(\s*)$/i.test(b.textContent || ''));
            if (btn) {
              btn.disabled = false;
              btn.removeAttribute('disabled');
              btn.setAttribute('aria-disabled', 'false');
            }
          }).catch(() => {});
          await page.waitForTimeout(300);
        }
      } catch {}
      
      // 点击 Sign 按钮
      console.log('[DEBUG] Attempting to click Sign button...');
      await waitForPageStable(page, 2000);
      const beforeClickUrl = page.url();
      
      try {
        // 使用找到的 frame 或主页面进行点击
        const targetPage = signBtnFrame || page;
        await safeClick(targetPage, signBtn, { timeout: 10000, force: true, retries: 3 });
        console.log('[DEBUG] Sign button clicked successfully!');
      } catch (e) {
        // 如果失败，尝试直接点击
        console.warn(`[SAFE-CLICK] Sign button click failed, trying direct click: ${e.message}`);
        try {
          await signBtn.click({ force: true, timeout: 5000 });
          console.log('[DEBUG] Sign button clicked via direct click!');
        } catch (e2) {
          // 如果还是失败，尝试使用 role 定位在正确的 frame 中
          console.warn(`[SAFE-CLICK] Direct click also failed, trying role method: ${e2.message}`);
          const targetPage = signBtnFrame || page;
          const btn2 = targetPage.getByRole('button', { name: /^sign$/i }).first();
          if (await btn2.isVisible({ timeout: 3000 }).catch(() => false)) {
            await safeClick(targetPage, btn2, { timeout: 10000, force: true, retries: 2 });
          } else {
            // 如果还是找不到，在所有 frame 中尝试
            for (const frame of page.frames()) {
              try {
                const frameBtn = frame.getByRole('button', { name: /^sign$/i }).first();
                if (await frameBtn.isVisible({ timeout: 1000 }).catch(() => false)) {
                  await safeClick(frame, frameBtn, { timeout: 10000, force: true, retries: 2 });
                  console.log('[DEBUG] Sign button clicked in alternative frame!');
                  break;
                }
              } catch {}
            }
          }
        }
      }
      
      // 等待一下看是否有页面变化或错误
      await page.waitForTimeout(2000);
      const afterClickUrl = page.url();
      console.log('[DEBUG] URL before click:', beforeClickUrl);
      console.log('[DEBUG] URL after click:', afterClickUrl);
    } else {
      console.error('[DEBUG] Sign button not found after all attempts!');
      throw new Error('Sign button not found on sign page');
    }
    
    // 检查是否被重定向到 429 错误页面（只有 429 才需要 disconnect）
    const signClickErrorPageCheck = await checkIfErrorPage(page);
    if (signClickErrorPageCheck.isErrorPage && signClickErrorPageCheck.errorCode === '429') {
      console.warn(`[429-ERROR] Detected 429 error page after Sign click: ${signClickErrorPageCheck.url}`);
      const handled = await handleErrorPage(page, BASE_URL);
      if (handled) {
        await page.waitForTimeout(2000);
        // 如果重置成功，检查是否在初始状态
        const isReset = await page.getByText('Enter an address manually', { exact: true }).isVisible({ timeout: 3000 }).catch(() => false);
        if (isReset) {
          console.log('[429-ERROR] Page reset after Sign click, needs to restart from beginning');
          throw new Error('429 error page detected after Sign click and reset, restart from beginning');
        }
      }
    }
    
    // 检查是否有错误信息（包括速率限制错误）
    const rateLimitCheck = await checkPageForRateLimitError(page);
    if (rateLimitCheck.hasError) {
      console.warn(`[RATE-LIMIT] Rate limit error detected after Sign click: ${rateLimitCheck.errorText}`);
      console.log(`[RATE-LIMIT] Waiting for rate limit to clear (max 20s)...`);
      const cleared = await waitForRateLimitErrorToClear(page, 20000, 3000, BASE_URL);
      if (!cleared) {
        // 如果仍有错误，检查是否在 429 错误页面（只有 429 才需要 disconnect）
        const finalErrorPageCheck = await checkIfErrorPage(page);
        if (finalErrorPageCheck.isErrorPage && finalErrorPageCheck.errorCode === '429') {
          console.log(`[429-ERROR] Still on 429 error page after waiting, attempting recovery via disconnect...`);
          const handled = await handleErrorPage(page, BASE_URL);
          if (handled) {
            await page.waitForTimeout(2000);
            // 检查是否重置成功
            const isReset = await page.getByText('Enter an address manually', { exact: true }).isVisible({ timeout: 3000 }).catch(() => false);
            if (isReset) {
              console.log('[ERROR-PAGE] Page reset after waiting, needs to restart from beginning');
              throw new Error('Error page detected after waiting and reset, restart from beginning');
            }
          }
        } else {
          // 尝试刷新页面
          console.log(`[RATE-LIMIT] Rate limit persists, refreshing page...`);
          try {
            await page.reload({ waitUntil: 'domcontentloaded', timeout: 15000 });
            await page.waitForTimeout(2000);
            
            // 刷新后检查是否跳转到错误页面
            const afterRefreshErrorCheck = await checkIfErrorPage(page);
            if (afterRefreshErrorCheck.isErrorPage) {
              await handleErrorPage(page, BASE_URL);
              await page.waitForTimeout(2000);
            }
            
            const afterReloadCheck = await checkPageForRateLimitError(page);
            if (afterReloadCheck.hasError) {
              // 即使刷新后还有错误，也继续执行
              console.warn(`[RATE-LIMIT] Error persists after refresh, but continuing anyway...`);
            }
          } catch (reloadError) {
            console.warn(`[RATE-LIMIT] Failed to refresh: ${reloadError.message}, continuing...`);
          }
        }
      }
    }
    
    // 检查其他错误信息
    const errorMsg = await page.evaluate(() => {
      const errorEls = document.querySelectorAll('[role="alert"], .error, .error-message, [class*="error"]');
      for (const el of errorEls) {
        const text = el.textContent || '';
        // 排除速率限制错误（已处理）
        if (text.trim().length > 0 && !/too many|rate limit/i.test(text)) {
          return text.trim();
        }
      }
      return null;
    }).catch(() => null);
    if (errorMsg) {
      console.log('[DEBUG] Page error detected:', errorMsg);
    }

    // 等待页面跳转或 Start 按钮出现（更灵活的方式）
    console.log('[DEBUG] Waiting for Start button or page navigation...');
    
    // 在等待过程中定期检查速率限制错误和429错误页面
    const waitForStartWithRateLimitCheck = async (maxWaitMs = 30000) => {
      const startTime = Date.now();
      const checkInterval = 2000; // 每2秒检查一次
      
      while (Date.now() - startTime < maxWaitMs) {
        // 首先检查是否在 429 错误页面（只有 429 才需要 disconnect）
        const errorPageCheck = await checkIfErrorPage(page);
        if (errorPageCheck.isErrorPage && errorPageCheck.errorCode === '429') {
          console.warn(`[429-ERROR] Detected 429 error page while waiting for Start button: ${errorPageCheck.url}`);
          const handled = await handleErrorPage(page, BASE_URL);
          if (handled) {
            await page.waitForTimeout(2000);
            // 如果重置成功，检查是否在初始状态
            const isReset = await page.getByText('Enter an address manually', { exact: true }).isVisible({ timeout: 3000 }).catch(() => false);
            if (isReset) {
              console.log('[429-ERROR] Page reset while waiting for Start, needs to restart from beginning');
              throw new Error('429 error page detected and reset, restart from beginning');
            }
          }
        }
        
        // 检查速率限制错误
        const check = await checkPageForRateLimitError(page, 1);
        if (check.hasError) {
          console.warn(`[RATE-LIMIT] Rate limit error detected while waiting for Start button: ${check.errorText}`);
          const cleared = await waitForRateLimitErrorToClear(page, 20000, 3000, BASE_URL);
          if (!cleared) {
            throw new Error(`Rate limit error while waiting for Start button: ${check.errorText}`);
          }
        }
        
        // 检查 Start 按钮是否出现
        const startButtonFound = await page.evaluate(() => {
          const btn = Array.from(document.querySelectorAll('button')).find(b => /^(\s*)start(\s*)$/i.test(b.textContent || ''));
          return !!btn;
        }).catch(() => false);
        
        if (startButtonFound) {
          return true;
        }
        
        await page.waitForTimeout(checkInterval);
      }
      
      return false;
    };
    
    // 等待网络请求完成（Sign 可能触发提交）
    await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
    
    // 等待 Start 按钮出现（带速率限制检查）
    await waitForStartWithRateLimitCheck(30000).catch(() => {
      console.warn('[DEBUG] Start button not found within timeout, continuing...');
    });
    
    // ⚠️ 检查是否已到达start session页面（已登录状态）
    // 已登录状态的定义：页面显示出"Solve cryptographic challenges"且页面里包含start session或stop session按钮
    const isLoggedInPage = await page.evaluate(() => {
      const bodyText = (document.body?.innerText || '').toLowerCase();
      // 检查是否显示"Solve cryptographic challenges"
      const hasSolveCryptoText = bodyText.includes('solve cryptographic challenges');
      
      if (!hasSolveCryptoText) {
        return false;
      }
      
      // 检查是否有start session或stop session按钮
      const allButtons = Array.from(document.querySelectorAll('button'));
      const hasStartButton = allButtons.some(b => {
        const text = b.textContent?.trim().toLowerCase();
        return (text === 'start' || text === 'start session') && b.offsetParent !== null;
      });
      const hasStopButton = allButtons.some(b => {
        const text = b.textContent?.trim().toLowerCase();
        return (text === 'stop' || text === 'stop session') && b.offsetParent !== null;
      });
      
      return hasStartButton || hasStopButton;
    }).catch(() => false);
    
    if (isLoggedInPage) {
      // 已到达start session页面（显示"Solve cryptographic challenges"且有start/stop按钮），从"登录阶段"转为"已登录状态"
      if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
        if (taskStats.loggingIn > 0) {
          taskStats.loggingIn--;
        }
        taskStats.loggedIn++;
        
        // ⚠️ 记录登录完成时间（到达start session页面的时间）
        const timer = taskStats.taskTimers.get(task.id);
        if (timer && timer.pageOpenTime) {
          timer.loginCompleteTime = Date.now();
          const loginTime = (timer.loginCompleteTime - timer.pageOpenTime) / 1000; // 转换为秒
          taskStats.loginTimes.push(loginTime);
          console.log(`[STATS] 📝 Task ${task.id} logged in (reached "Solve cryptographic challenges" page, Logged In: ${taskStats.loggedIn}, Login Time: ${loginTime.toFixed(2)}s)`);
        } else {
          console.log(`[STATS] 📝 Task ${task.id} logged in (reached "Solve cryptographic challenges" page, Logged In: ${taskStats.loggedIn})`);
        }
      }
    }
    
    // 点击 Start 按钮（多种策略，失败也不抛出错误，因为监控脚本会在后台自动处理）
    let startClicked = false;
    const startBtn = page.locator('button:has-text("Start")').first();
    // 等待页面稳定，使用安全点击
    await waitForPageStable(page, 2000);
    
    if (await startBtn.isVisible({ timeout: 5000 }).catch(() => false)) {
      try {
        // ⚠️ 点击start按钮前，离开"已登录状态"，记录挖矿开始时间
        if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
          if (taskStats.loggedIn > 0) {
            taskStats.loggedIn--;
          }
          
          // ⚠️ 记录挖矿开始时间（点击start按钮的时间）
          const timer = taskStats.taskTimers.get(task.id);
          if (timer) {
            timer.miningStartTime = Date.now();
          }
        }
        await safeClick(page, startBtn, { timeout: 10000, force: true, retries: 3 });
        console.log('[DEBUG] Start button clicked successfully!');
        startClicked = true;
      } catch (e) {
        console.log('[DEBUG] Failed to click Start button (method 1):', String(e));
      }
    }
    
    if (!startClicked) {
      // 尝试用 role 定位
      try {
        // ⚠️ 点击start按钮前（如果还未点击），离开"已登录状态"，记录挖矿开始时间
        if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
          if (taskStats.loggedIn > 0 && !startClicked) {
            taskStats.loggedIn--;
          }
          
          // ⚠️ 记录挖矿开始时间（点击start按钮的时间）
          const timer = taskStats.taskTimers.get(task.id);
          if (timer && !timer.miningStartTime) {
            timer.miningStartTime = Date.now();
          }
        }
        const startBtnRole = page.getByRole('button', { name: /^start$/i });
        await safeClick(page, startBtnRole, { timeout: 10000, force: true, retries: 3 });
        console.log('[DEBUG] Start button clicked via role!');
        startClicked = true;
      } catch (e) {
        console.log('[DEBUG] Failed to click Start button (method 2):', String(e));
      }
    }
    
    if (!startClicked) {
      console.log('[WARN] Could not click Start button initially, monitor will auto-click when session starts');
    }

    // 任务完成前：检查是否成功建立了挖矿 session（是否有 Start/Stop 按钮）
    const checkMiningSessionEstablished = async () => {
      try {
        await page.waitForTimeout(3000); // 等待页面稳定
        
        // 首先检查是否还在 Terms 页面（如果是，说明没有成功离开）
        const stillInTerms = await (async () => {
          try {
            const text = await page.evaluate(() => document.body?.innerText || '').catch(() => '');
            if (/Accept Token End User Terms/i.test(text) || /TOKEN END-USER TERMS/i.test(text)) {
              // 检查是否还有 Accept and Sign 按钮
              const hasAcceptBtn = await page.getByRole('button', { name: /Accept and Sign/i }).first().isVisible({ timeout: 500 }).catch(() => false);
              if (hasAcceptBtn) {
                return true; // 还在 Terms 页面
              }
            }
            
            // 检查所有 frame
            for (const f of page.frames()) {
              try {
                const frameText = await f.evaluate(() => document.body?.innerText || '').catch(() => '');
                if (/Accept Token End User Terms/i.test(frameText) || /TOKEN END-USER TERMS/i.test(frameText)) {
                  const frameAcceptBtn = f.getByRole('button', { name: /Accept and Sign/i }).first();
                  if (await frameAcceptBtn.isVisible({ timeout: 500 }).catch(() => false)) {
                    return true; // 还在 Terms 页面
                  }
                }
              } catch {}
            }
            
            return false;
          } catch (e) {
            return false;
          }
        })();
        
        // 首先检查是否有 Start/Stop 按钮（挖矿 session）- 这是最重要的检查
        // 如果有 session，说明已经成功完成了流程，不需要检查是否在 Terms 页面
        const hasSession = await page.evaluate(() => {
          const allButtons = Array.from(document.querySelectorAll('button'));
          const hasStart = allButtons.some(b => {
            const text = b.textContent?.trim().toLowerCase();
            return (text === 'start' || text === 'start session') && b.offsetParent !== null;
          });
          const hasStop = allButtons.some(b => {
            const text = b.textContent?.trim().toLowerCase();
            return (text === 'stop' || text === 'stop session') && b.offsetParent !== null;
          });
          return hasStart || hasStop; // 有 Start 或 Stop 按钮表示 session 已建立
        }).catch(() => false);
        
        if (hasSession) {
          console.log('[SESSION-CHECK] ✓ Mining session established successfully (Start/Stop buttons found)');
          // ⚠️ 重要：有 session 不代表任务完成，需要检查状态
          // 只有当状态显示为 "waiting for the next challenge" 时，task 才算作 completed
          // 如果状态是 "finding a solution"，表示正在挖矿，需要继续等待
          
          // 检查当前状态
          const currentStatus = await page.evaluate(() => {
            const bodyText = (document.body?.innerText || '').toLowerCase();
            if (bodyText.includes('waiting for the next challenge')) {
              return 'waiting for the next challenge'; // ✅ 任务已完成
            } else if (bodyText.includes('finding a solution') || bodyText.includes('finding')) {
              return 'finding a solution'; // ⛏️ 任务正在进行中（挖矿中）
            }
            return 'unknown';
          }).catch(() => 'unknown');
          
          if (currentStatus === 'waiting for the next challenge') {
            // ⚠️ 记录挖矿完成时间（状态变成waiting for the next challenge的时间）
            if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
              const timer = taskStats.taskTimers.get(task.id);
              if (timer && timer.miningStartTime && !timer.miningCompleteRecorded) {
                const miningCompleteTime = Date.now();
                const miningTime = (miningCompleteTime - timer.miningStartTime) / 1000; // 转换为秒
                taskStats.miningTimes.push(miningTime);
                timer.miningCompleteRecorded = true; // 标记已记录，避免重复记录
                console.log(`[SESSION-CHECK] ✓ Task ${task.id} completed: Status is "waiting for the next challenge" (Mining Time: ${miningTime.toFixed(2)}s)`);
              } else {
                console.log('[SESSION-CHECK] ✓ Task completed: Status is "waiting for the next challenge"');
              }
            } else {
              console.log('[SESSION-CHECK] ✓ Task completed: Status is "waiting for the next challenge"');
            }
            return; // 任务已完成
          } else if (currentStatus === 'finding a solution') {
            console.log('[SESSION-CHECK] ⏳ Task is mining: Status is "finding a solution", waiting for "waiting for the next challenge"...');
            console.log('[SESSION-CHECK] ℹ️ No timeout set - will wait until status changes (mining difficulty varies by cycle)');
            // ⚠️ 更新统计：任务已开始挖矿
            if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
              taskStats.miningStarted++;
              console.log(`[STATS] ⛏️ Task ${task.id} started mining (Active Mining: ${taskStats.miningStarted})`);
            }
            // ⚠️ 不设置超时时间，因为不同周期的挖矿难度不一样，可能很快也可能很慢
            // 持续等待直到状态变成 "waiting for the next challenge"
            // 使用轮询方式持续检查状态（每2秒检查一次）
            while (true) {
              const status = await page.evaluate(() => {
                const bodyText = (document.body?.innerText || '').toLowerCase();
                return bodyText.includes('waiting for the next challenge') ? 'completed' : 
                       (bodyText.includes('finding a solution') || bodyText.includes('finding')) ? 'mining' : 'unknown';
              }).catch(() => 'unknown');
              
              if (status === 'completed') {
                // ⚠️ 记录挖矿完成时间（状态变成waiting for the next challenge的时间）
                if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
                  const timer = taskStats.taskTimers.get(task.id);
                  if (timer && timer.miningStartTime && !timer.miningCompleteRecorded) {
                    const miningCompleteTime = Date.now();
                    const miningTime = (miningCompleteTime - timer.miningStartTime) / 1000; // 转换为秒
                    taskStats.miningTimes.push(miningTime);
                    timer.miningCompleteRecorded = true; // 标记已记录，避免重复记录
                    console.log(`[SESSION-CHECK] ✓ Task ${task.id} completed: Status changed to "waiting for the next challenge" (Mining Time: ${miningTime.toFixed(2)}s)`);
                  } else {
                    console.log('[SESSION-CHECK] ✓ Task completed: Status changed to "waiting for the next challenge"');
                  }
                } else {
                  console.log('[SESSION-CHECK] ✓ Task completed: Status changed to "waiting for the next challenge"');
                }
                return; // 任务已完成
              } else if (status !== 'mining') {
                // 状态异常（既不是mining也不是completed），记录日志但继续等待
                console.warn(`[SESSION-CHECK] ⚠️ Unexpected status: ${status}, continuing to wait...`);
              }
              
              // 等待2秒后再次检查
              await page.waitForTimeout(2000);
            }
          } else {
            console.log('[SESSION-CHECK] ✓ Session established, but status unknown. Task initialized successfully.');
            // session 已建立，虽然没有检测到明确状态，但至少流程走完了
            return;
          }
        }
        
        // 如果没有 session，才检查是否还在 Terms 页面
        if (stillInTerms) {
          console.error('[SESSION-CHECK] Still in Terms page and no mining session found! Attempting to recover...');
          
          // 策略1: 尝试使用 Reset session 按钮恢复
          console.log('[SESSION-CHECK] Trying Reset session button first...');
          const resetSuccess = await resetSessionAndReturn(page);
          if (resetSuccess) {
            console.log('[SESSION-CHECK] ✓ Successfully reset via Reset session, task will restart from beginning');
            throw new Error('Reset via Reset session, task needs to restart from beginning');
          }
          
          // 策略2: 尝试最后一次点击 "Accept and Sign" 按钮
          try {
            const acceptBtn = page.getByRole('button', { name: /Accept and Sign/i }).first();
            if (await acceptBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
              console.log('[SESSION-CHECK] Found "Accept and Sign" button, attempting final click...');
              await waitForPageStable(page, 2000);
              await safeClick(page, acceptBtn, { timeout: 10000, force: true, retries: 2 });
              
              // 等待 10 秒看是否离开 Terms 页面或建立了 session
              await page.waitForTimeout(10000);
              
              // 再次检查是否有 session（优先）
              const hasSessionAfterRetry = await page.evaluate(() => {
                const allButtons = Array.from(document.querySelectorAll('button'));
                const hasStart = allButtons.some(b => {
                  const text = b.textContent?.trim().toLowerCase();
                  return (text === 'start' || text === 'start session') && b.offsetParent !== null;
                });
                const hasStop = allButtons.some(b => {
                  const text = b.textContent?.trim().toLowerCase();
                  return (text === 'stop' || text === 'stop session') && b.offsetParent !== null;
                });
                return hasStart || hasStop;
              }).catch(() => false);
              
              if (hasSessionAfterRetry) {
                console.log('[SESSION-CHECK] ✓ Mining session established after retry!');
                // ⚠️ 重要：检查状态，只有当状态是 "waiting for the next challenge" 才算完成
                const currentStatusAfterRetry = await page.evaluate(() => {
                  const bodyText = (document.body?.innerText || '').toLowerCase();
                  if (bodyText.includes('waiting for the next challenge')) {
                    return 'waiting for the next challenge';
                  } else if (bodyText.includes('finding a solution') || bodyText.includes('finding')) {
                    return 'finding a solution';
                  }
                  return 'unknown';
                }).catch(() => 'unknown');
                
                if (currentStatusAfterRetry === 'waiting for the next challenge') {
                  // ⚠️ 记录挖矿完成时间（重试后状态是waiting for the next challenge）
                  if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
                    const timer = taskStats.taskTimers.get(task.id);
                    if (timer && timer.miningStartTime && !timer.miningCompleteRecorded) {
                      const miningCompleteTime = Date.now();
                      const miningTime = (miningCompleteTime - timer.miningStartTime) / 1000; // 转换为秒
                      taskStats.miningTimes.push(miningTime);
                      timer.miningCompleteRecorded = true;
                      console.log(`[SESSION-CHECK] ✓ Task ${task.id} completed after retry: Status is "waiting for the next challenge" (Mining Time: ${miningTime.toFixed(2)}s)`);
                    } else {
                      console.log('[SESSION-CHECK] ✓ Task completed after retry: Status is "waiting for the next challenge"');
                    }
                  } else {
                    console.log('[SESSION-CHECK] ✓ Task completed after retry: Status is "waiting for the next challenge"');
                  }
                  return; // 任务已完成
                } else if (currentStatusAfterRetry === 'finding a solution') {
                  console.log('[SESSION-CHECK] ⏳ Task is mining after retry: Status is "finding a solution", waiting for "waiting for the next challenge"...');
                  console.log('[SESSION-CHECK] ℹ️ No timeout set - will wait until status changes (mining difficulty varies by cycle)');
                  // ⚠️ 更新统计：任务已开始挖矿
                  if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
                    taskStats.miningStarted++;
                    console.log(`[STATS] ⛏️ Task ${task.id} started mining after retry (Active Mining: ${taskStats.miningStarted})`);
                  }
                  // ⚠️ 不设置超时时间，因为不同周期的挖矿难度不一样，可能很快也可能很慢
                  // 持续等待直到状态变成 "waiting for the next challenge"
                  // 使用轮询方式持续检查状态（每2秒检查一次）
                  while (true) {
                    const status = await page.evaluate(() => {
                      const bodyText = (document.body?.innerText || '').toLowerCase();
                      return bodyText.includes('waiting for the next challenge') ? 'completed' : 
                             (bodyText.includes('finding a solution') || bodyText.includes('finding')) ? 'mining' : 'unknown';
                    }).catch(() => 'unknown');
                    
                    if (status === 'completed') {
                      // ⚠️ 记录挖矿完成时间（重试后轮询中状态变成waiting for the next challenge）
                      if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
                        const timer = taskStats.taskTimers.get(task.id);
                        if (timer && timer.miningStartTime && !timer.miningCompleteRecorded) {
                          const miningCompleteTime = Date.now();
                          const miningTime = (miningCompleteTime - timer.miningStartTime) / 1000; // 转换为秒
                          taskStats.miningTimes.push(miningTime);
                          timer.miningCompleteRecorded = true;
                          console.log(`[SESSION-CHECK] ✓ Task ${task.id} completed after retry: Status changed to "waiting for the next challenge" (Mining Time: ${miningTime.toFixed(2)}s)`);
                        } else {
                          console.log('[SESSION-CHECK] ✓ Task completed after retry: Status changed to "waiting for the next challenge"');
                        }
                      } else {
                        console.log('[SESSION-CHECK] ✓ Task completed after retry: Status changed to "waiting for the next challenge"');
                      }
                      return; // 任务已完成
                    } else if (status !== 'mining') {
                      // 状态异常（既不是mining也不是completed），记录日志但继续等待
                      console.warn(`[SESSION-CHECK] ⚠️ Unexpected status after retry: ${status}, continuing to wait...`);
                    }
                    
                    // 等待2秒后再次检查
                    await page.waitForTimeout(2000);
                  }
                } else {
                  console.log('[SESSION-CHECK] ✓ Session established after retry, but status unknown. Task initialized successfully.');
                  return; // session 已建立，至少流程走完了
                }
              }
              
              // 再次检查是否还在 Terms 页面
              const stillInTermsAfterRetry = await (async () => {
                try {
                  const text = await page.evaluate(() => document.body?.innerText || '').catch(() => '');
                  if (/Accept Token End User Terms/i.test(text) || /TOKEN END-USER TERMS/i.test(text)) {
                    return await acceptBtn.isVisible({ timeout: 500 }).catch(() => false);
                  }
                  return false;
                } catch {
                  return false;
                }
              })();
              
              if (stillInTermsAfterRetry) {
                console.error('[SESSION-CHECK] Still in Terms page after final retry, task failed');
                throw new Error('Still in Terms page, failed to complete the flow even after retries');
              } else {
                console.log('[SESSION-CHECK] ✓ Successfully left Terms page after final retry, but no session yet');
                // 已经离开 Terms 页面，但还没有 session，可能流程还在进行中
                // 不抛出错误，继续后续检查
              }
            } else {
              throw new Error('Still in Terms page, but "Accept and Sign" button not found');
          }
        } catch (e) {
            console.error(`[SESSION-CHECK] Recovery attempt failed: ${e.message}`);
            throw new Error('Still in Terms page, failed to complete the flow');
          }
        }
        
        // 如果既没有 session 也不在 Terms 页面，检查是否在其他错误状态
        if (!hasSession) {
          console.error('[SESSION-CHECK] Mining session not established (no Start/Stop buttons found)!');
          // 没有建立 session，抛出错误，让任务标记为失败
          throw new Error('Mining session not established - no Start/Stop buttons found');
        }
      } catch (e) {
        console.error(`[SESSION-CHECK] Error: ${e.message}`);
        throw e; // 重新抛出错误，让任务标记为失败
      }
    };
    
    // ⚠️ 如果只是初始化模式，跳过session完成检查，直接注册到scheduler
    if (initOnly && scheduler) {
      // 验证是否已到达"Solve cryptographic challenges"页面（有start session按钮）
      const hasStartButton = await page.evaluate(() => {
        const bodyText = (document.body?.innerText || '').toLowerCase();
        // 检查是否显示"Solve cryptographic challenges"
        const hasSolveCryptoText = bodyText.includes('solve cryptographic challenges');
        
        if (!hasSolveCryptoText) {
          return false;
        }
        
        // 检查是否有start session按钮
        const allButtons = Array.from(document.querySelectorAll('button'));
        const hasStart = allButtons.some(b => {
          const text = b.textContent?.trim().toLowerCase();
          return (text === 'start' || text === 'start session') && b.offsetParent !== null && !b.disabled;
        });
        return hasStart;
      }).catch(() => false);
      
      if (hasStartButton) {
        console.log(`[INIT] Task ${task.id} initialized, registering with scheduler (not starting mining)`);
        await scheduler.addTask(task.id, page, browser);
      } else {
        // 如果还没有到达start session页面，等待并检查一次session状态（但不等待完成）
        console.log(`[INIT] Task ${task.id} not yet at start session page, checking session establishment...`);
        // 只检查session是否建立，不等待完成
        const sessionEstablished = await page.evaluate(() => {
          const allButtons = Array.from(document.querySelectorAll('button'));
          const hasStart = allButtons.some(b => {
            const text = b.textContent?.trim().toLowerCase();
            return (text === 'start' || text === 'start session') && b.offsetParent !== null;
          });
          const hasStop = allButtons.some(b => {
            const text = b.textContent?.trim().toLowerCase();
            return (text === 'stop' || text === 'stop session') && b.offsetParent !== null;
          });
          return hasStart || hasStop;
        }).catch(() => false);
        
        if (sessionEstablished) {
          console.log(`[INIT] Task ${task.id} initialized (session established), registering with scheduler (not starting mining)`);
          await scheduler.addTask(task.id, page, browser);
        } else {
          // 如果session还没建立，等待一小段时间后再次检查
          await page.waitForTimeout(3000);
          const sessionEstablishedAfterWait = await page.evaluate(() => {
            const allButtons = Array.from(document.querySelectorAll('button'));
            const hasStart = allButtons.some(b => {
              const text = b.textContent?.trim().toLowerCase();
              return (text === 'start' || text === 'start session') && b.offsetParent !== null;
            });
            const hasStop = allButtons.some(b => {
              const text = b.textContent?.trim().toLowerCase();
              return (text === 'stop' || text === 'stop session') && b.offsetParent !== null;
            });
            return hasStart || hasStop;
          }).catch(() => false);
          
          if (sessionEstablishedAfterWait) {
            console.log(`[INIT] Task ${task.id} initialized (session established after wait), registering with scheduler (not starting mining)`);
            await scheduler.addTask(task.id, page, browser);
          } else {
            throw new Error(`Task ${task.id} failed to establish session during initialization`);
          }
        }
      }
      // 标记任务为READY状态
      const taskData = scheduler.tasks.get(task.id);
      if (taskData) {
        taskData.status = 'ready';
      }
      // 在初始化模式下，不注入自动启动监控，由scheduler统一管理
      return { id: task.id, ok: true, initialized: true };
    }

    // 注入监控：当 session 停掉（按钮从 Stop 变回 Start）时自动点击 Start 继续（仅在非初始化模式）
    await page.evaluate(() => {
      // 使用 Map 跟踪每个按钮对的状态（支持多个 session）
      const buttonStates = new Map(); // key: button element, value: { lastState: 'running'|'stopped', lastClickTime: number }
      const CLICK_THROTTLE_MS = 3000; // 至少间隔3秒才能再次点击
      
      // 生成按钮的唯一标识（用于 Map key）
      const getButtonKey = (btn) => {
        if (!btn) return null;
        // 使用位置和文本生成唯一标识
        try {
          const rect = btn.getBoundingClientRect();
          const text = btn.textContent?.trim() || '';
          const parent = btn.closest('div, section, form, [class*="card"], [class*="session"]');
          const parentId = parent ? (parent.id || parent.className || '') : '';
          return `${text}_${Math.round(rect.top)}_${Math.round(rect.left)}_${parentId.substring(0, 50)}`;
        } catch {
          return `${btn.textContent?.trim()}_${Date.now()}`;
        }
      };
      
      // 查找所有按钮对（Start 和 Stop 按钮）
      const findButtonPairs = () => {
        const allButtons = Array.from(document.querySelectorAll('button'));
        const pairs = [];
        
        // 找到所有 Start 和 Stop 按钮
        const startButtons = allButtons.filter(b => {
          if (b.disabled) return false;
          const text = b.textContent?.trim().toLowerCase();
          return (text === 'start' || text === 'start session') && b.offsetParent !== null; // offsetParent !== null 表示可见
        });
        
        const stopButtons = allButtons.filter(b => {
          if (b.disabled) return false;
          const text = b.textContent?.trim().toLowerCase();
          return (text === 'stop' || text === 'stop session') && b.offsetParent !== null;
        });
        
        // 为每个 Stop 按钮找到对应的 Start 按钮（通过位置关系）
        stopButtons.forEach(stopBtn => {
          const stopRect = stopBtn.getBoundingClientRect();
          const stopParent = stopBtn.closest('div, section, form, [class*="card"], [class*="session"]');
          
          // 找最近的 Start 按钮（在同一个容器或附近）
          const nearbyStart = startButtons.find(startBtn => {
            const startParent = startBtn.closest('div, section, form, [class*="card"], [class*="session"]');
            // 检查是否在同一个父容器中
            const sameParent = stopParent === startParent;
            // 或者位置相近（垂直距离小于 200px，水平距离小于 500px）
            const startRect = startBtn.getBoundingClientRect();
            const nearby = Math.abs(startRect.top - stopRect.top) < 200 && 
                          Math.abs(startRect.left - stopRect.left) < 500;
            return sameParent || nearby;
          });
          
          if (nearbyStart) {
            // 使用统一的位置key（不依赖文本）
            const unifiedKey = (() => {
              try {
                const rect = stopBtn.getBoundingClientRect();
                const parent = stopBtn.closest('div, section, form, [class*="card"], [class*="session"]');
                const parentId = parent ? (parent.id || parent.className || '') : '';
                return `${Math.round(rect.top / 10)}_${Math.round(rect.left / 10)}_${parentId.substring(0, 50)}`;
              } catch {
                return getButtonKey(stopBtn);
              }
            })();
            pairs.push({ startBtn: nearbyStart, stopBtn, key: unifiedKey });
          } else {
            // 只有 Stop 按钮，没有对应的 Start（可能是同一个按钮的不同状态）
            const unifiedKey = (() => {
              try {
                const rect = stopBtn.getBoundingClientRect();
                const parent = stopBtn.closest('div, section, form, [class*="card"], [class*="session"]');
                const parentId = parent ? (parent.id || parent.className || '') : '';
                return `${Math.round(rect.top / 10)}_${Math.round(rect.left / 10)}_${parentId.substring(0, 50)}`;
              } catch {
                return getButtonKey(stopBtn);
              }
            })();
            pairs.push({ startBtn: null, stopBtn, key: unifiedKey });
          }
        });
        
        // 对于没有对应 Stop 按钮的 Start 按钮，也创建单独的记录（这些是已停止的 session）
        // 注意：Start 和 Stop 可能是同一个按钮，只是文本在变化
        startButtons.forEach(startBtn => {
          const hasPair = pairs.some(p => p.startBtn === startBtn || p.stopBtn === startBtn);
          if (!hasPair) {
            // 检查是否可能是同一个按钮的不同状态（通过位置匹配）
            const startRect = startBtn.getBoundingClientRect();
            const startParent = startBtn.closest('div, section, form, [class*="card"], [class*="session"]');
            const samePositionStop = stopButtons.find(stopBtn => {
              const stopRect = stopBtn.getBoundingClientRect();
              const stopParent = stopBtn.closest('div, section, form, [class*="card"], [class*="session"]');
              return stopParent === startParent && 
                     Math.abs(stopRect.top - startRect.top) < 5 && 
                     Math.abs(stopRect.left - startRect.left) < 5;
            });
            
            // 使用统一的位置作为key，不依赖文本
            const unifiedKey = (() => {
              try {
                const rect = startBtn.getBoundingClientRect();
                const parent = startBtn.closest('div, section, form, [class*="card"], [class*="session"]');
                const parentId = parent ? (parent.id || parent.className || '') : '';
                // 去掉文本前缀，只使用位置信息
                return `${Math.round(rect.top / 10)}_${Math.round(rect.left / 10)}_${parentId.substring(0, 50)}`;
              } catch {
                return getButtonKey(startBtn);
              }
            })();
            
            pairs.push({ startBtn, stopBtn: samePositionStop || null, key: unifiedKey });
          }
        });
        
        return pairs;
      };
      
      const checkAndAutoStart = () => {
        const pairs = findButtonPairs();
        
        pairs.forEach(({ startBtn, stopBtn, key }) => {
          if (!key) return;
          
          // 确定当前状态（优先检查 Stop 按钮，因为 Stop 按钮存在表示正在运行）
          let currentState = null;
          if (stopBtn && stopBtn.offsetParent !== null) {
            currentState = 'running';
          } else if (startBtn && startBtn.offsetParent !== null) {
            currentState = 'stopped';
          }
          
          if (currentState === null) {
            // 按钮都不可见，清理状态记录
            buttonStates.delete(key);
            return;
          }
          
          // 获取或创建该按钮对的状态记录
          let stateRecord = buttonStates.get(key);
          
          if (!stateRecord) {
            // 首次检测，初始化状态（不触发自动点击）
            stateRecord = { 
              lastState: currentState, 
              lastClickTime: 0,
              startBtn,
              stopBtn
            };
            buttonStates.set(key, stateRecord);
            console.log(`[AUTO-START] Initialized button pair (${key.substring(0, 30)}...): ${currentState}`);
            return;
          }
          
          // 更新按钮引用（防止 DOM 变化导致引用失效）
          stateRecord.startBtn = startBtn;
          stateRecord.stopBtn = stopBtn;
          
          // 检测状态变化：从 running 变为 stopped
          if (stateRecord.lastState === 'running' && currentState === 'stopped' && startBtn) {
            const now = Date.now();
            // 节流检查：确保不在短时间内重复点击
            if (now - stateRecord.lastClickTime >= CLICK_THROTTLE_MS && !startBtn.disabled && startBtn.offsetParent !== null) {
              console.log(`[AUTO-START] Session stopped detected (${key.substring(0, 30)}...), auto-clicking Start...`);
              try {
                // 滚动到按钮位置，确保可见
                startBtn.scrollIntoView({ behavior: 'smooth', block: 'center' });
                setTimeout(() => {
          startBtn.click();
                  stateRecord.lastClickTime = Date.now();
                  // 点击后，不立即更新 lastState，让下次检查时自然更新
                  // 这样可以确保如果点击没有生效，下次还能检测到并重试
                  console.log('[AUTO-START] Start button clicked successfully');
                }, 200);
              } catch (e) {
                console.error('[AUTO-START] Error clicking Start button:', e);
              }
            }
          } else {
            // 正常更新状态记录（包括从 stopped 变为 running 的情况）
            stateRecord.lastState = currentState;
          }
        });
        
        // 清理不再存在的按钮对的状态记录
        const currentKeys = new Set(pairs.map(p => p.key).filter(Boolean));
        for (const key of buttonStates.keys()) {
          if (!currentKeys.has(key)) {
            console.log(`[AUTO-START] Removing stale button state: ${key.substring(0, 30)}...`);
            buttonStates.delete(key);
          }
        }
      };
      
      // 轮询检查（每 1.5 秒检查一次，提高响应速度）
      const intervalId = setInterval(checkAndAutoStart, 1500);
      
      // MutationObserver 监听 DOM 变化，实时响应按钮状态变化
      const mo = new MutationObserver(() => {
        // DOM 变化时立即检查
        checkAndAutoStart();
      });
      mo.observe(document.body, { 
        subtree: true, 
        childList: true, 
        characterData: true,
        attributes: true,
        attributeFilter: ['class', 'disabled', 'aria-disabled', 'style', 'hidden'] // 监听按钮启用/禁用状态变化
      });
      
      // 立即执行一次检查，初始化所有按钮对的状态
      checkAndAutoStart();
      
      console.log('[AUTO-START] Monitor installed: will auto-click Start when session stops (supports multiple sessions)');
      
      // 清理函数（如果需要的话）
      window.__autoStartCleanup = () => {
        clearInterval(intervalId);
        mo.disconnect();
        buttonStates.clear();
      };
    });

    // 可选择在此维持一段时间或直到某条件成立
    // await page.waitForTimeout(60000);

    return { id: task.id, ok: true };
  } catch (e) {
    // 如果是速率限制错误，记录详细信息
    const errorMsg = String(e);
    if (errorMsg.toLowerCase().includes('rate limit') || errorMsg.toLowerCase().includes('too many')) {
      console.error(`[RATE-LIMIT] Task ${task.id} failed due to rate limit error: ${errorMsg}`);
      // 不立即返回失败，而是等待一段时间后重试（通过外部的重试机制）
    }
    
    // ⚠️ 如果初始化失败且还没有注册到调度器，需要关闭页面和浏览器以释放资源
    if (initOnly && !scheduler?.tasks.has(task.id)) {
      console.warn(`[RUNONE] Task ${task.id} failed before registration, closing page and browser to free resources...`);
      try {
        if (page && !page.isClosed()) {
          await page.close().catch(() => {});
        }
      } catch (closeErr) {
        // 忽略关闭错误
      }
      try {
        if (browser) {
          await browser.close().catch(() => {});
        }
      } catch (closeErr) {
        // 忽略关闭错误
      }
    }
    
    return { id: task.id, ok: false, error: String(e) };
  } finally {
    // 停止后台监控
    stopRateLimitMonitor();
    // ⚠️ 注意：如果任务已成功注册到调度器，页面和浏览器由调度器管理，不应在这里关闭
    // 只有在初始化失败且未注册到调度器时，才会在 catch 块中关闭
  }
}

// 全局统计对象
const taskStats = {
  total: 0,
  completed: 0,
  success: 0,
  failed: 0,
  miningStarted: 0, // 已开始挖矿的任务数（状态为"finding a solution"）
  loggingIn: 0, // 登录阶段：页面已打开但还未到达start session页面
  loggedIn: 0, // 已登录状态：已到达start session页面但还未点击start按钮
  loginTimes: [], // 登录时间数组（从打开页面到start session页面的时间，单位：秒）
  miningTimes: [], // 挖矿时间数组（从点击start session到状态变成waiting的时间，单位：秒）
  taskTimers: new Map(), // 每个任务的时间记录 { taskId: { pageOpenTime, loginCompleteTime, miningStartTime } }
  lastUpdateTime: Date.now(),
};

// 定期输出统计信息
// ⚠️ 在调度器模式下，这个统计信息不应该输出（调度器有自己的状态报告）
function logTaskStats() {
  // 如果是在调度器模式下运行，不输出统计信息（调度器有自己的状态报告）
  if (process.env.SCHEDULED_MODE === 'true' || process.env.RUN_SCHEDULED === 'true') {
    return; // 调度器模式下禁用此统计输出
  }
  
  const now = Date.now();
  const elapsed = Math.floor((now - taskStats.lastUpdateTime) / 1000);
  const successRate = taskStats.completed > 0 ? (taskStats.success / taskStats.completed * 100).toFixed(1) : '0.0';
  const remaining = taskStats.total - taskStats.completed;
  
  console.log('\n' + '='.repeat(60));
  console.log(`[STATS] Task Statistics (updated every 10 seconds)`);
  console.log(`  Total Tasks: ${taskStats.total}`);
  console.log(`  Completed: ${taskStats.completed} (${successRate}% success)`);
  console.log(`  ✓ Success (Completed): ${taskStats.success}`);
  console.log(`  ✗ Failed: ${taskStats.failed}`);
  console.log(`  Remaining: ${remaining}`);
  console.log(`  🔐 Logging In (before start session page): ${taskStats.loggingIn}`);
  console.log(`  📝 Logged In (at start session page): ${taskStats.loggedIn}`);
  // 当前正在挖矿的任务数 = 已开始挖矿的 - 已完成的
  const currentlyMining = Math.max(0, taskStats.miningStarted - taskStats.success);
  console.log(`  ⛏️ Active Mining (currently mining): ${currentlyMining}`);
  
  // ⚠️ 计算并显示平均登录时间和平均挖矿时间
  const avgLoginTime = taskStats.loginTimes.length > 0 
    ? (taskStats.loginTimes.reduce((sum, t) => sum + t, 0) / taskStats.loginTimes.length).toFixed(2)
    : '0.00';
  const avgMiningTime = taskStats.miningTimes.length > 0
    ? (taskStats.miningTimes.reduce((sum, t) => sum + t, 0) / taskStats.miningTimes.length).toFixed(2)
    : '0.00';
  console.log(`  📊 Avg Login Time: ${avgLoginTime}s (from ${taskStats.loginTimes.length} tasks)`);
  console.log(`  📊 Avg Mining Time: ${avgMiningTime}s (from ${taskStats.miningTimes.length} tasks)`);
  
  console.log(`  Last Update: ${elapsed}s ago`);
  console.log('='.repeat(60) + '\n');
}

async function runWithConcurrency() {
  const results = [];
  const queue = tasks.slice();
  let running = 0;
  
  // 初始化统计
  taskStats.total = tasks.length;
  taskStats.completed = 0;
  taskStats.success = 0;
  taskStats.failed = 0;
  taskStats.miningStarted = 0;
  taskStats.loggingIn = 0;
  taskStats.loggedIn = 0;
  taskStats.loginTimes = [];
  taskStats.miningTimes = [];
  taskStats.taskTimers.clear();
  taskStats.lastUpdateTime = Date.now();
  
  // 启动定期统计输出（每10秒）
  const statsInterval = setInterval(() => {
    logTaskStats();
  }, 10000);
  
  // 初始输出（只在非调度器模式下）
  if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
    console.log(`[STATS] Starting ${taskStats.total} tasks with concurrency ${CONCURRENCY}`);
  }

  return new Promise((resolve) => {
    const launchNext = () => {
      if (queue.length === 0 && running === 0) {
        clearInterval(statsInterval);
        // 最终统计
        logTaskStats();
        resolve(results);
        return;
      }
      while (running < CONCURRENCY && queue.length > 0) {
        const task = queue.shift();
        running++;
        // ⚠️ 不设置超时，因为挖矿任务需要在点击start session后等待状态变成"waiting for the next challenge"才算完成
        // 不同周期的挖矿难度不一样，可能很快也可能很慢
        const p = runOne(task);
        p.then(r => {
          results.push(r);
          taskStats.completed++;
          
          if (r.ok) {
            taskStats.success++;
            // 只在非调度器模式下输出详细日志
            if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
              console.log(`[STATS] ✓ Task ${r.id} completed successfully (Total: ${taskStats.success}/${taskStats.total})`);
            }
          } else {
            taskStats.failed++;
            // 只在非调度器模式下输出详细日志
            if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
              console.log(`[STATS] ✗ Task ${r.id} failed: ${r.error?.substring(0, 50) || 'unknown error'} (Failed: ${taskStats.failed}/${taskStats.total})`);
            }
          }
          
          // ⚠️ 任务完成或失败时，清理状态计数
          if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
            // 检查任务是否在挖矿中（通过检查taskTimers）
            const timer = taskStats.taskTimers.get(r.id);
            const wasMining = timer && timer.miningStartTime && !timer.miningCompleteRecorded;
            
            // 清理状态计数
            if (taskStats.loggingIn > 0) {
              taskStats.loggingIn--;
            }
            if (taskStats.loggedIn > 0) {
              taskStats.loggedIn--;
            }
            // ⚠️ 如果任务已经开始挖矿（无论成功还是失败），都需要清理miningStarted计数
            // 因为任务完成了（成功）或失败了，不再处于挖矿状态
            if (wasMining && taskStats.miningStarted > 0) {
              taskStats.miningStarted--;
            }
            
            // 清理任务时间记录
            taskStats.taskTimers.delete(r.id);
          }
          // 每完成一个任务也更新统计（只在非调度器模式下）
          if ((process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') && 
              (taskStats.completed % 5 === 0 || taskStats.completed === taskStats.total)) {
            logTaskStats();
          }
        })
         .catch(e => {
           results.push({ id: task.id, ok: false, error: String(e) });
           taskStats.completed++;
           taskStats.failed++;
           
           // ⚠️ 任务失败时，清理状态计数
           if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
             // 检查任务是否在挖矿中（通过检查taskTimers）
             const timer = taskStats.taskTimers.get(task.id);
             const wasMining = timer && timer.miningStartTime && !timer.miningCompleteRecorded;
             
             // 清理状态计数
             if (taskStats.loggingIn > 0) {
               taskStats.loggingIn--;
             }
             if (taskStats.loggedIn > 0) {
               taskStats.loggedIn--;
             }
             // ⚠️ 如果任务失败但已经开始挖矿，需要清理miningStarted计数
             if (wasMining && taskStats.miningStarted > 0) {
               taskStats.miningStarted--;
             }
             
             // 清理任务时间记录
             taskStats.taskTimers.delete(task.id);
           }
           
           // 只在非调度器模式下输出详细日志
           if (process.env.SCHEDULED_MODE !== 'true' && process.env.RUN_SCHEDULED !== 'true') {
             console.log(`[STATS] ✗ Task ${task.id} error: ${String(e).substring(0, 50)} (Failed: ${taskStats.failed}/${taskStats.total})`);
           }
         })
         .finally(() => {
           running--;
           launchNext();
         });
      }
    };
    launchNext();
  });
}

// 导出函数供其他模块使用
// 注意：BASE_URL 和 SIGN_SERVICE_URL 已经在上面通过 export const 导出了
export { runOne, runOneInitOnly, loadTasks };

// 如果不是被导入，则运行默认流程
// 检查是否是直接运行的脚本（不是被import）
// ⚠️ 在调度器模式下（SCHEDULED_MODE 或 RUN_SCHEDULED），不执行 runWithConcurrency
const isMainModule = process.argv[1] && import.meta.url === `file://${process.argv[1]}`;
const isScheduledMode = process.env.SCHEDULED_MODE === 'true' || process.env.RUN_SCHEDULED === 'true';

// 只在直接运行脚本且非调度器模式下才执行 runWithConcurrency
if (isMainModule && !isScheduledMode) {
  runWithConcurrency().then(res => {
    console.log('Done:', res);
    const failed = res.filter(r => !r.ok);
    if (failed.length) {
      console.error('Failed:', failed);
      process.exitCode = 1;
    }
  });
}