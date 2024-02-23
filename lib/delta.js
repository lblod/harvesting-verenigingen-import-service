import flatten from 'lodash.flatten';

export class Delta {

  constructor(delta) {
    this.delta = delta;
  }

  get inserts() {
    return flatten(this.delta.map(changeSet => changeSet.inserts));
  }

  getInsertsFor(predicate, object) {
    return this.inserts
      .filter(t => t.predicate.value === predicate && t.object.value === object)
      .map(t => t.subject.value);
  }

}