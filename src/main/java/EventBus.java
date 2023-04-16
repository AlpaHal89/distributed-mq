import com.google.inject.Inject;
import com.google.inject.Singleton;
import exceptions.RetryLimitExceededException;
import exceptions.UnsubscribedPollException;
import lib.KeyedExecutor;
import models.Event;
import models.FailureEvent;
import models.Subscription;
import util.Timer;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;

@Singleton
public class EventBus {
    //Distributed MQ
    //always use list when we want to get by index(offset) + concurrency for read and write using locks
    //list doesnt have
    private final Map<String, List<Event>> topics;
    private final KeyedExecutor<String> eventExecutor;
    private final KeyedExecutor<String> broadcastExecutor;
    private EventBus deadLetterQueue; //this is addln feature

    //vimp. index to store for every topic store the CHM of offsetNum,event
    //CHM is fine here cause reads are
    private final Map<String, Map<Integer, Event>> eventOffsetIndexes;
    private final Map<String, ConcurrentSkipListMap<Long, String>> eventTimestamps;

    //vimp. 2 types of subscriptions. topic-<sub name,sub object>
    private final Map<String, Map<String, Subscription>> pullSubscriptions;
    private final Map<String, Map<String, Subscription>> pushSubscriptions;
    private final Timer timer;

    @Inject
    public EventBus(final KeyedExecutor<String> eventExecutor, final KeyedExecutor<String> broadcastExecutor, final Timer timer) {
        this.topics = new ConcurrentHashMap<>();
        this.eventOffsetIndexes = new ConcurrentHashMap<>();
        this.eventTimestamps = new ConcurrentHashMap<>();
        this.pullSubscriptions = new ConcurrentHashMap<>();
        this.pushSubscriptions = new ConcurrentHashMap<>();
        this.eventExecutor = eventExecutor;
        this.broadcastExecutor = broadcastExecutor;
        this.timer = timer;
    }

    //A. set for dead letter Q
    public void setDeadLetterQueue(final EventBus deadLetterQueue) {
        this.deadLetterQueue = deadLetterQueue;
    }

    //B. subscribe/unsubscribe
    private void subscribeForPushEvents(final String topic,
                                        final String subscriber,
                                        final Predicate<Event> precondition,
                                        final Function<Event, CompletionStage<Void>> handler,
                                        final int numberOfRetries) {
        addSubscriber(pushSubscriptions, subscriber, precondition, topic, handler, numberOfRetries);
    }

    private void subscribeForPullEvents(final String topic, final String subscriber, final Predicate<Event> precondition) {
        addSubscriber(pullSubscriptions, subscriber, precondition, topic, null, 0);
    }

    private void addSubscriber(final Map<String, Map<String, Subscription>> subscriptions,
                               final String subscriber,
                               final Predicate<Event> precondition,
                               final String topic,
                               final Function<Event, CompletionStage<Void>> handler,
                               final int numberOfRetries) {
        //subscription has topic,sub name,callback for subscription if required,retries always at consumer level
        final var subscription = new Subscription(topic, subscriber, precondition, handler, numberOfRetries);
        subscription.setCurrentIndex(topics.getOrDefault(topic, new CopyOnWriteArrayList<>()).size()); //vimp.offset starts at size.getordefault get from new val
        subscriptions.computeIfAbsent(topic, k -> new ConcurrentHashMap<>()).put(subscriber, subscription); //add to new value
    }

    private void unsubscribeFromTopic(final String topic, final String subscriber) {
        pushSubscriptions.getOrDefault(topic, new HashMap<>()).remove(subscriber);
        pullSubscriptions.getOrDefault(topic, new HashMap<>()).remove(subscriber);
    }

    //publish. returning CF with no value
    private CompletionStage<Void> publishToBus(final String topic, final Event event) {
        if (topics.get(topic).contains(event)) {
            return null;
        }
        //1. add event to topic
        //2. add to index for topic key with value as ridx for sorted time,event
        //3. push to subscribers if topic has push subscribers
        topics.computeIfAbsent(topic,k -> new LinkedHashSet<>()).add(event);
        eventOffsetIndexes.computeIfAbsent(topic, k -> new ConcurrentHashMap<>()).put(topics.get(topic).size()-1, event);
        //eventTimestamps.computeIfAbsent(topic, k -> new ConcurrentSkipListMap<>()).put(timer.getCurrentTime(), event.getId());
        return notifyPushSubscribers(topic, event);
    }

    private CompletionStage<Void> notifyPushSubscribers(String topic, Event event) {
        if (!pushSubscriptions.containsKey(topic)) {
            return CompletableFuture.completedStage(null);
        }
        final var subscribersForTopic = pushSubscriptions.get(topic);
        final var notifications = subscribersForTopic.values()
                .stream()
                .filter(subscription -> subscription.getPrecondition().test(event)) //test() for predicate. can skip this
                .map(subscription -> executeEventHandler(event, subscription))// if reqd
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(notifications); //vimp. allOf waits on CF[]
    }

    private CompletionStage<Void> executeEventHandler(final Event event, Subscription subscription) {
        return broadcastExecutor.getThreadFor(subscription.getTopic() + subscription.getSubscriber(),
                doWithRetry(event, subscription.getEventHandler(),
                        1, subscription.getNumberOfRetries())
                        .exceptionally(throwable -> { //handle errors only. add to dead letter Q failure event with exception and time
                            if (deadLetterQueue != null) {
                                deadLetterQueue.publish(subscription.getTopic(), new FailureEvent(event, throwable, timer.getCurrentTime()));
                            }
                            return null; //need return in exceptionally
                        }));
    }

    private CompletionStage<Void> doWithRetry(final Event event,
                                              final Function<Event, CompletionStage<Void>> task,
                                              final int coolDownIntervalInMillis,
                                              final int remainingTries) {
        return task.apply(event).handle((__, throwable) -> {
            if (throwable != null) { //retry if error
                if (remainingTries == 0) {
                    //custom exception for business purposes: checked extends Exception, unchecked extends RuntimeEx(prefer)
                    //standard is add 1 constructor that has error message and 'throwable' and call super() with the 2
                    throw new RetryLimitExceededException(throwable);
                }
                try {
                    //only way to pause current thread execution. but may drift and not be exact due to other code execution time
                    TimeUnit.MILLISECONDS.sleep(coolDownIntervalInMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e); //rethrow exception/reset interrupt flag
                }
                return doWithRetry(event, task, Math.max(coolDownIntervalInMillis * 2, 10), remainingTries - 1); //retrytime*2 or max 10
            } else {
                return CompletableFuture.completedFuture((Void) null); //weird!!
            }
        }).thenCompose(Function.identity()); //flatmap CF
    }

    //pull messages in batch/single
    private Event pollBus(final String topic, final String subscriber) {
        var subscription = pullSubscriptions.getOrDefault(topic, new HashMap<>()).get(subscriber);
        if (subscription == null) {
            throw new UnsubscribedPollException(); //custom ex
        }
        //start reading from consumer current offset till end of topic. offset is longAdder cause we could have multiple consumer instances!!
        for (var index = subscription.getCurrentIndex(); index.intValue() < topics.get(topic).size(); index.increment()) {
            var event = topics.get(topic).get(index.intValue());
            if (subscription.getPrecondition().test(event)) {
                index.increment();
                return event;
            }
        }
        return null;
    }

    //move offset to specific event say to resume after after server restart
    private void moveIndexAfterEvent(final String topic, final String subscriber, final String eventId) {
        if (eventId == null) {
            pullSubscriptions.get(topic).get(subscriber).setCurrentIndex(0);
        } else {
            final var eventIndex = eventOffsetIndexes.get(topic).get(eventId) + 1;
            pullSubscriptions.get(topic).get(subscriber).setCurrentIndex(eventIndex);
        }
    }

    //if we want same key(topic+consumer) to go to same server/thread i.e. sequence their calls subscribe,unsubscribe,poll,moveOffset
    public CompletionStage<Event> poll(final String topic, final String subscriber) {
        return eventExecutor.getThreadFor(topic + subscriber, () -> pollBus(topic, subscriber));
    }

    //skip methods•••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    public CompletionStage<Void> subscribeToEventsAfter(final String topic, final String subscriber, final String eventId) {
        return eventExecutor.getThreadFor(topic + subscriber, () -> moveIndexAfterEvent(topic, subscriber, eventId));
    }

    public CompletionStage<Void> unsubscribe(final String topic, final String subscriber) {
        return eventExecutor.getThreadFor(topic + subscriber, () -> unsubscribeFromTopic(topic, subscriber));
    }

    public CompletionStage<Void> subscribeForPush(final String topic,
                                                  final String subscriber,
                                                  final Predicate<Event> precondition,
                                                  final Function<Event, CompletionStage<Void>> handler,
                                                  final int numberOfRetries) {
        return eventExecutor.getThreadFor(topic + subscriber,
                () -> subscribeForPushEvents(topic, subscriber, precondition, handler, numberOfRetries));
    }

    public CompletionStage<Void> subscribeForPull(final String topic, final String subscriber, final Predicate<Event> precondition) {
        return eventExecutor.getThreadFor(topic + subscriber, () -> subscribeForPullEvents(topic, subscriber, precondition));
    }

    public CompletionStage<Void> publish(final String topic, final Event event) {
        return eventExecutor.getThreadFor(topic, publishToBus(topic, event));
    }

    public CompletionStage<Void> subscribeToEventsAfter(final String topic, final String subscriber, final long timeStamp) {
        return eventExecutor.getThreadFor(topic + subscriber, () -> moveIndexAtTimestamp(topic, subscriber, timeStamp));
    }

    private void moveIndexAtTimestamp(final String topic, final String subscriber, final long timeStamp) {
        final var closestEventAfter = eventTimestamps.get(topic).higherEntry(timeStamp);
        if (closestEventAfter == null) {
            pullSubscriptions.get(topic).get(subscriber).setCurrentIndex(eventOffsetIndexes.get(topic).size());
        } else {
            final var eventIndex = eventOffsetIndexes.get(topic).get(closestEventAfter.getValue());
            pullSubscriptions.get(topic).get(subscriber).setCurrentIndex(eventIndex);
        }
    }
}

