<script setup>
import { RouterLink } from 'vue-router'
import { useNavigation } from '@/composables/useNavigation'

const { stepStates } = useNavigation()
</script>

<template>
  <div class="flyover-navigation">
    <div class="container">
      <div class="nav-steps">
        <RouterLink
          id="home-step"
          to="/"
          class="nav-step"
        >
          <span class="step-number">
            <i class="fas fa-home step-icon-header" />
          </span>
          <span class="step-text">Home</span>
        </RouterLink>

        <span class="step-separator">|</span>

        <template
          v-for="(step, idx) in stepStates"
          :key="step.name"
        >
          <RouterLink
            :id="`${step.name}-step`"
            :to="step.to"
            class="nav-step"
            :class="{
              active: step.active,
              completed: step.completed,
              disabled: step.disabled,
            }"
            @click.prevent="step.disabled ? null : null"
          >
            <span class="step-number">
              <i
                class="fas step-icon-header"
                :class="step.completed ? step.completedIcon : step.icon"
              />
            </span>
            <span class="step-text">{{ step.label }}</span>
          </RouterLink>
          <i
            v-if="idx < stepStates.length - 1"
            class="fas fa-chevron-right step-arrow"
          />
        </template>
      </div>
    </div>
  </div>
</template>
