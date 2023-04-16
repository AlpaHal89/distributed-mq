package models;

import java.util.Objects;
import java.util.UUID;

public class Event {
    private final String id;
    private final String publisher; //why we ned this?
    private final EventType eventType; //priority for event
    private final String description; //this is the content
    private final long creationTime; //imp to store msg time

    public Event(final String publisher,
                 final EventType eventType,
                 final String description,
                 final long creationTime) {
        this.description = description;
        this.id = UUID.randomUUID().toString();
        this.publisher = publisher;
        this.eventType = eventType;
        this.creationTime = creationTime;
    }

    public String getId() {
        return id;
    }

    public String getPublisher() {
        return publisher;
    }

    public EventType getEventType() {
        return eventType;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return id.equals(event.id) && eventType == event.eventType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, eventType);
    }
}