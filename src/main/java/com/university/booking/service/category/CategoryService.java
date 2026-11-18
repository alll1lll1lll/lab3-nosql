package com.university.booking.service.category;

import com.university.booking.dto.category.CategoryRequest;
import com.university.booking.events.EventDraft;
import com.university.booking.events.EventPayload;
import com.university.booking.events.EventRecorder;
import com.university.booking.events.EventType;
import com.university.booking.model.Category;
import com.university.booking.repository.CategoryRepository;
import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class CategoryService {

    private final CategoryRepository categoryRepository;
    private final EventRecorder events;

    @CacheEvict(value = "categories", allEntries = true)
    public Category createCategory(CategoryRequest request) {
        Category category = Category.builder()
                .id(UUID.randomUUID().toString())
                .name(request.getName())
                .description(request.getDescription())
                .build();

        Category saved = categoryRepository.save(category);
        events.record(EventDraft.of(EventType.CATEGORY_CREATED, saved.getId(),
                EventPayload.of("name", saved.getName(), "description", saved.getDescription())));
        return saved;
    }

    @Cacheable(value = "categories")
    public List<Category> getAllCategories() {
        return categoryRepository.findAll();
    }

    @CacheEvict(value = "categories", allEntries = true)
    public void deleteCategory(String id) {
        var deleted = categoryRepository.findById(id);
        categoryRepository.deleteById(id);
        deleted.ifPresent(c -> events.record(EventDraft.of(EventType.CATEGORY_DELETED, id,
                EventPayload.of("name", c.getName()))));
    }
}