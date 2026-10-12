import { Component, OnInit, OnDestroy, ChangeDetectorRef, inject } from '@angular/core';
import { DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { buildDays, buildMonthCells, dateKey, sanitizeSavedPlan, serializePlanCsv, STORAGE_KEY, type PlannerDay, type PlanEntry } from './planner';

@Component({
  selector: 'app-date-list',
  standalone: true,
  imports: [DatePipe, FormsModule],
  templateUrl: './main.html',
  styleUrls: ['./main.component.css']
})
export class DateListComponent implements OnInit, OnDestroy {
  days = buildDays(new Date());
  selectedDay = this.days[0];
  activeMonth = new Date(this.days[0].date.getFullYear(), this.days[0].date.getMonth(), 1, 12);
  entries: Record<string, PlanEntry> = {};
  view: 'calendar' | 'list' = 'calendar';
  draftNote = '';
  statusMessage = '';
  storageWarning = '';
  weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
  private changeDetector = inject(ChangeDetectorRef);
  private midnightTimer?: ReturnType<typeof setTimeout>;
  private readonly handleVisibility = () => { if (!document.hidden) this.refreshDates(); };

  ngOnInit(): void {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      this.entries = sanitizeSavedPlan(saved ? JSON.parse(saved) : {});
    } catch {
      this.storageWarning = 'Browser storage is unavailable. You can still plan and export this session.';
    }
    this.draftNote = this.entries[this.selectedDay.key]?.note ?? '';
    document.addEventListener('visibilitychange', this.handleVisibility);
    this.scheduleMidnightRefresh();
  }

  ngOnDestroy(): void {
    clearTimeout(this.midnightTimer);
    document.removeEventListener('visibilitychange', this.handleVisibility);
  }

  get cells() { return buildMonthCells(this.activeMonth, this.days); }
  get plannedCount() { return this.days.filter(day => this.entries[day.key]?.note.trim()).length; }
  get completedCount() { return this.days.filter(day => this.entries[day.key]?.completed).length; }
  get progress() { return Math.round(this.completedCount / 30 * 100); }
  get selectedEntry(): PlanEntry | undefined { return this.entries[this.selectedDay.key]; }
  get upcoming() { return this.days.filter(day => this.entries[day.key]?.note.trim()).slice(0, 3); }
  get canPrevious() { return this.monthIndex(this.activeMonth) > this.monthIndex(this.days[0].date); }
  get canNext() { return this.monthIndex(this.activeMonth) < this.monthIndex(this.days[29].date); }

  selectDay(day: PlannerDay): void {
    this.selectedDay = day;
    this.draftNote = this.entries[day.key]?.note ?? '';
    this.statusMessage = '';
    this.activeMonth = new Date(day.date.getFullYear(), day.date.getMonth(), 1, 12);
  }

  shiftMonth(direction: number): void {
    if ((direction < 0 && !this.canPrevious) || (direction > 0 && !this.canNext)) return;
    this.activeMonth = new Date(this.activeMonth.getFullYear(), this.activeMonth.getMonth() + direction, 1, 12);
  }

  goToday(): void {
    this.refreshDates();
    this.selectDay(this.days[0]);
  }

  saveNote(): void {
    const note = this.draftNote.trim().slice(0, 2000);
    this.entries = { ...this.entries, [this.selectedDay.key]: { note, completed: this.selectedEntry?.completed ?? false } };
    this.draftNote = note;
    const saved = this.persist();
    this.statusMessage = saved ? (note ? 'Note saved.' : 'Note cleared.') : 'Note updated for this session. Export to keep a copy.';
  }

  toggleCompleted(): void {
    const current = this.selectedEntry;
    this.entries = { ...this.entries, [this.selectedDay.key]: { note: current?.note ?? '', completed: !current?.completed } };
    const saved = this.persist();
    this.statusMessage = this.selectedEntry?.completed ? 'Day marked complete.' : 'Day marked incomplete.';
    if (!saved) this.statusMessage += ' Export to keep a copy.';
  }

  exportPlan(): void {
    const blob = new Blob(['\uFEFF' + serializePlanCsv(this.days, this.entries)], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = `next30-${this.days[0].key}.csv`;
    anchor.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    this.statusMessage = 'Your 30-day plan was exported.';
  }

  private persist(): boolean {
    this.entries = sanitizeSavedPlan(this.entries);
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(this.entries));
      this.storageWarning = '';
      return true;
    } catch {
      this.storageWarning = 'Your changes could not be saved in this browser. Export your plan to keep a copy.';
      return false;
    }
  }

  private monthIndex(date: Date): number { return date.getFullYear() * 12 + date.getMonth(); }

  private refreshDates(): void {
    if (dateKey(new Date()) === this.days[0].key) return;
    const selectedKey = this.selectedDay.key;
    this.days = buildDays(new Date());
    this.selectDay(this.days.find(day => day.key === selectedKey) ?? this.days[0]);
    this.scheduleMidnightRefresh();
    this.changeDetector.markForCheck();
  }

  private scheduleMidnightRefresh(): void {
    clearTimeout(this.midnightTimer);
    const now = new Date();
    const next = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
    this.midnightTimer = setTimeout(() => { this.refreshDates(); this.scheduleMidnightRefresh(); }, next.getTime() - now.getTime() + 100);
  }
}
