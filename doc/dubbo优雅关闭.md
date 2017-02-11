---

**dubbo的优雅关闭**

 - spring的Lifecycle接口、Phased、DefaultLifecycleProcessor
 - java并发CountDownLatch


 1.  com.alibaba.dubbo.container.Container接口  
``` stylus
public interface Container {
    
    /**
     * start.
     */
    void start();
    
    /**
     * stop.
     */
    void stop();

}
```
2. public class SpringContainer implements Container

``` stylus
    public void stop() {
        try {
            if (context != null) {
                context.stop();
                context.close();
                context = null;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }
```

3. context.stop

``` stylus
	public void stop() {
		getLifecycleProcessor().stop();
		publishEvent(new ContextStoppedEvent(this));
	}
```
4. public class DefaultLifecycleProcessor implements LifecycleProcessor, BeanFactoryAware

``` stylus
	/**
	 * Stop all registered beans that implement Lifecycle and <i>are</i>
	 * currently running. Any bean that implements SmartLifecycle
	 * will be stopped within its 'phase', and all phases will be ordered
	 * from highest to lowest value. All beans that do not implement
	 * SmartLifecycle will be stopped in the default phase 0. A bean
	 * declared as dependent on another bean will be stopped before
	 * the dependency bean regardless of the declared phase.
	 */
	private void stopBeans() {
		Map<String, Lifecycle> lifecycleBeans = getLifecycleBeans();
		Map<Integer, LifecycleGroup> phases = new HashMap<Integer, LifecycleGroup>();
		for (Map.Entry<String, Lifecycle> entry : lifecycleBeans.entrySet()) {
			Lifecycle bean = entry.getValue();
			int shutdownOrder = getPhase(bean);
			LifecycleGroup group = phases.get(shutdownOrder);
			if (group == null) {
				group = new LifecycleGroup(shutdownOrder, this.timeoutPerShutdownPhase, lifecycleBeans, false);
				phases.put(shutdownOrder, group);
			}
			group.add(entry.getKey(), bean);
		}
		if (phases.size() > 0) {
			List<Integer> keys = new ArrayList<Integer>(phases.keySet());
			Collections.sort(keys, Collections.reverseOrder());
			for (Integer key : keys) {
				phases.get(key).stop();
			}
		}
	}
```
5. LifecycleGroup

``` stylus
		public void stop() {
			if (this.members.isEmpty()) {
				return;
			}
			if (logger.isInfoEnabled()) {
				logger.info("Stopping beans in phase " + this.phase);
			}
			Collections.sort(this.members, Collections.reverseOrder());
			CountDownLatch latch = new CountDownLatch(this.smartMemberCount);
			Set<String> countDownBeanNames = Collections.synchronizedSet(new LinkedHashSet<String>());
			for (LifecycleGroupMember member : this.members) {
				if (this.lifecycleBeans.containsKey(member.name)) {
					doStop(this.lifecycleBeans, member.name, latch, countDownBeanNames);
				}
				else if (member.bean instanceof SmartLifecycle) {
					// already removed, must have been a dependent
					latch.countDown();
				}
			}
			try {
				latch.await(this.timeout, TimeUnit.MILLISECONDS);
				if (latch.getCount() > 0 && !countDownBeanNames.isEmpty() && logger.isWarnEnabled()) {
					logger.warn("Failed to shut down " + countDownBeanNames.size() + " bean" +
							(countDownBeanNames.size() > 1 ? "s" : "") + " with phase value " +
							this.phase + " within timeout of " + this.timeout + ": " + countDownBeanNames);
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}
```


``` stylus
	/**
	 * Stop the specified bean as part of the given set of Lifecycle beans,
	 * making sure that any beans that depends on it are stopped first.
	 * @param lifecycleBeans Map with bean name as key and Lifecycle instance as value
	 * @param beanName the name of the bean to stop
	 */
	private void doStop(Map<String, ? extends Lifecycle> lifecycleBeans, final String beanName,
			final CountDownLatch latch, final Set<String> countDownBeanNames) {

		Lifecycle bean = lifecycleBeans.remove(beanName);
		if (bean != null) {
			String[] dependentBeans = this.beanFactory.getDependentBeans(beanName);
			for (String dependentBean : dependentBeans) {
				doStop(lifecycleBeans, dependentBean, latch, countDownBeanNames);
			}
			try {
				if (bean.isRunning()) {
					if (bean instanceof SmartLifecycle) {
						if (logger.isDebugEnabled()) {
							logger.debug("Asking bean '" + beanName + "' of type [" + bean.getClass() + "] to stop");
						}
						countDownBeanNames.add(beanName);
						((SmartLifecycle) bean).stop(new Runnable() {
							public void run() {
								latch.countDown();
								countDownBeanNames.remove(beanName);
								if (logger.isDebugEnabled()) {
									logger.debug("Bean '" + beanName + "' completed its stop procedure");
								}
							}
						});
					}
					else {
						if (logger.isDebugEnabled()) {
							logger.debug("Stopping bean '" + beanName + "' of type [" + bean.getClass() + "]");
						}
						bean.stop();
						if (logger.isDebugEnabled()) {
							logger.debug("Successfully stopped bean '" + beanName + "'");
						}
					}
				}
				else if (bean instanceof SmartLifecycle) {
					// don't wait for beans that aren't running
					latch.countDown();
				}
			}
			catch (Throwable ex) {
				if (logger.isWarnEnabled()) {
					logger.warn("Failed to stop bean '" + beanName + "'", ex);
				}
			}
		}
	}
```
