package com.university.booking.service.person;

import com.university.booking.dto.person.PersonRequest;
import com.university.booking.enums.PersonRole;
import com.university.booking.events.EventDraft;
import com.university.booking.events.EventPayload;
import com.university.booking.events.EventRecorder;
import com.university.booking.events.EventType;
import com.university.booking.exception.AccessDeniedException;
import com.university.booking.exception.ResourceNotFoundException;
import com.university.booking.model.Person;
import com.university.booking.repository.PersonRepository;
import com.university.booking.service.security.AccessService;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.AllArgsConstructor;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class PersonService {

    private final PersonRepository personRepository;
    private final AccessService accessService;
    private final PersonLookupService personLookupService;
    private final EventRecorder events;

    public Person createPerson(PersonRequest request, String currentPersonId) {
        if (request.getRole() == PersonRole.ADMIN) {
            accessService.requireAdmin(currentPersonId);
        }

        Person person = Person.builder()
                .isuId(generateUniqueId())
                .lastName(request.getLastName())
                .firstName(request.getFirstName())
                .middleName(request.getMiddleName())
                .role(request.getRole())
                .build();

        Person saved = personRepository.save(person);
        events.record(EventDraft.of(EventType.PERSON_CREATED, saved.getIsuId(),
                currentPersonId != null ? currentPersonId : saved.getIsuId(),
                EventPayload.of("role", saved.getRole(), "lastName", saved.getLastName(),
                        "firstName", saved.getFirstName())));
        return saved;
    }

    public Person getPerson(String id, String currentPersonId) {
        if (!id.equals(currentPersonId) && !accessService.isAdmin(currentPersonId)) {
            throw new AccessDeniedException("Можно просматривать только свой профиль");
        }
        return personLookupService.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException(id));
    }

    public List<Person> getAllPersons() {
        return personRepository.findAll();
    }

    @CacheEvict(cacheNames = PersonLookupService.CACHE_NAME, key = "#p0")
    public void deletePerson(String id) {
        var deleted = personRepository.findById(id);
        personRepository.deleteById(id);
        deleted.ifPresent(p -> events.record(EventDraft.of(EventType.PERSON_DELETED, id,
                EventPayload.of("role", p.getRole()))));
    }

    private String generateUniqueId() {
        String id;
        do {
            id = String.valueOf(ThreadLocalRandom.current().nextInt(100000, 1000000));
        } while (personRepository.existsById(id));
        return id;
    }
}